package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
)

type bug04Transport struct {
	calls  atomic.Int32
	cancel context.CancelFunc
}

func (transport *bug04Transport) RoundTrip(*http.Request) (*http.Response, error) {
	if transport.calls.Add(1) == 2 {
		transport.cancel()
	}
	payload := []byte(`{"code":10007,"msg":"watch timeout","data":null}`)
	return &http.Response{StatusCode: http.StatusRequestTimeout, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
}

func TestBug04_WatchLoopPreservesTypedTimeoutError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transport := &bug04Transport{cancel: cancel}
	client := New("http://lockd.invalid")
	client.SetHTTPClient(&http.Client{Transport: transport})
	err := client.WatchLoop(ctx, "n", "l", nil)
	if transport.calls.Load() < 2 {
		t.Fatalf("watch loop stopped after %d timeout response", transport.calls.Load())
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("watch loop returned %v, want context cancellation", err)
	}
}
