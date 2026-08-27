package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// renewingTransport records each renew POST and returns a fresh expiry.
type renewingTransport struct {
	calls      atomic.Int32
	acquireHit atomic.Int32
}

func (t *renewingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch {
	case strings.HasSuffix(req.URL.Path, "/acquire"):
		t.acquireHit.Add(1)
		payload, _ := json.Marshal(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{
			"token": "tk", "holder": "h", "depth": 1,
			"expires_at": time.Now().Add(2 * time.Second),
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
	case strings.HasSuffix(req.URL.Path, "/renew"):
		t.calls.Add(1)
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		payload, _ := json.Marshal(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{
			"expires_at": time.Now().Add(2 * time.Second),
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
	}
	return nil, http.ErrAbortHandler
}

// TestRenewerSurvivesRequestContextCancellation reproduces the context-coupling
// defect: Acquire is performed under a short-lived request context, then the
// renewer is started under a fresh long-lived context. The renewer must keep
// issuing renewals after the Acquire request context expires, instead of dying
// with it and leaving the lease to drift idle.
func TestRenewerSurvivesRequestContextCancellation(t *testing.T) {
	transport := &renewingTransport{}
	c := New("http://lockd.invalid")
	c.SetHTTPClient(&http.Client{Transport: transport})

	acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	lease, err := c.Acquire(acquireCtx, "n", "l", "h", AcquireOptions{TTL: 1 * time.Second})
	acquireCancel()
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Give the short request context time to expire.
	time.Sleep(100 * time.Millisecond)
	if acquireCtx.Err() == nil {
		t.Fatal("precondition: acquire context should have expired")
	}

	renewerCtx, renewerCancel := context.WithCancel(context.Background())
	defer renewerCancel()
	stop := c.StartRenewer(renewerCtx, lease, func(error) { t.Errorf("onLost fired unexpectedly") })
	defer stop()

	// The renewer ticks at ~TTL/3 with jitter; wait long enough for several
	// renewals to land well after the Acquire context died.
	time.Sleep(700 * time.Millisecond)
	if got := transport.calls.Load(); got == 0 {
		t.Fatalf("renewer sent no renewals after request context expired; lease would drift idle")
	}
}

// TestRenewerStopsOnParentCancel keeps the ability to proactively stop the
// renewer: cancelling the parent context (or the returned stop func) must end
// the loop.
func TestRenewerStopsOnParentCancel(t *testing.T) {
	transport := &renewingTransport{}
	c := New("http://lockd.invalid")
	c.SetHTTPClient(&http.Client{Transport: transport})

	lease := &Lease{Namespace: "n", Name: "l", Holder: "h", Token: "tk", TTL: 300 * time.Millisecond}
	renewerCtx, renewerCancel := context.WithCancel(context.Background())
	stop := c.StartRenewer(renewerCtx, lease, nil)

	// Let at least one renewal fire, then cancel the parent.
	time.Sleep(200 * time.Millisecond)
	before := transport.calls.Load()
	renewerCancel()

	// After cancellation, no further renewals should arrive.
	time.Sleep(400 * time.Millisecond)
	after := transport.calls.Load()
	stop()
	if after != before {
		t.Fatalf("renewer kept running after parent cancel: %d -> %d", before, after)
	}
}

// TestRenewUsesCallerContext ensures Renew honors the caller-supplied context
// rather than a previously-remembered request context.
func TestRenewUsesCallerContext(t *testing.T) {
	transport := &renewingTransport{}
	c := New("http://lockd.invalid")
	c.SetHTTPClient(&http.Client{Transport: transport})

	lease := &Lease{Namespace: "n", Name: "l", Holder: "h", Token: "tk", TTL: 1 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Renew(ctx, lease)
	if err == nil {
		t.Fatal("expected Renew to fail under a cancelled context")
	}
}
