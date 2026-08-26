package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	core "lockd/internal/lock"
	"lockd/internal/logger"
	"lockd/internal/metrics"
)

func TestBug01_AcquireCancellationRemovesWaiter(t *testing.T) {
	service := core.NewService(core.NewRegistry(nil, 10, time.Second), core.NewBus(), &metrics.Metrics{}, "secret")
	if _, err := service.Create(core.CreateOptions{Namespace: "n", Name: "l", DefaultTTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Acquire(context.Background(), "n", "l", core.AcquireOptions{Holder: "owner"}); err != nil {
		t.Fatal(err)
	}

	handler := New(service, logger.New(io.Discard, "error"), nil).WithRequestTimeout(500 * time.Millisecond).Handler()
	body, err := json.Marshal(map[string]any{"holder": "waiter", "wait": true, "wait_timeout": "500ms"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", "/api/v1/locks/n/l/acquire", bytes.NewReader(body)).WithContext(ctx)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, req)
		close(done)
	}()

	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		view, getErr := service.Get("n", "l")
		if getErr != nil {
			t.Fatal(getErr)
		}
		if view.QueueLength == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter was not queued")
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	removed := false
	deadline = time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		view, getErr := service.Get("n", "l")
		if getErr != nil {
			t.Fatal(getErr)
		}
		if view.QueueLength == 0 {
			removed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !removed {
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("cancelled request remained in the wait queue")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acquire handler did not return after request cancellation")
	}
}
