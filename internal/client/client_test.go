package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestMutationRequestIsNotRetried(t *testing.T) {
	client := New("http://lockd.invalid")
	client.retries = 3
	var calls atomic.Int32
	client.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("connection lost after write")
	})})
	err := client.do(context.Background(), http.MethodPost, "/api/v1/locks/n/l/acquire", map[string]string{"holder": "one"}, "", nil)
	if err == nil {
		t.Fatal("expected transport error")
	}
	if calls.Load() != 1 {
		t.Fatalf("mutation was replayed %d times", calls.Load())
	}
}

func TestReadOnlyRequestRetainsRetry(t *testing.T) {
	client := New("http://lockd.invalid")
	client.retries = 2
	var calls atomic.Int32
	client.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("temporary network error")
	})})
	_ = client.do(context.Background(), http.MethodGet, "/api/v1/locks", nil, "", nil)
	if calls.Load() != 3 {
		t.Fatalf("read-only request attempts = %d, want 3", calls.Load())
	}
}

func TestDefaultClientAllowsLongPoll(t *testing.T) {
	client := New("http://127.0.0.1:8080")
	if client.httpClient.Timeout <= 10*time.Minute {
		t.Fatalf("HTTP timeout %s is shorter than server wait ceiling", client.httpClient.Timeout)
	}
}

func TestAcquireUsesServerDefaultTTLForRenewer(t *testing.T) {
	expiresAt := time.Now().Add(2 * time.Minute)
	payload, _ := json.Marshal(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{
		"token": "tk_test", "holder": "holder", "depth": 1, "expires_at": expiresAt,
	}})
	client := New("http://lockd.invalid")
	client.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
	})})
	lease, err := client.Acquire(context.Background(), "n", "l", "holder", AcquireOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.TTL < 119*time.Second {
		t.Fatalf("client ignored server default TTL: %s", lease.TTL)
	}
}
