package lock

import (
	"context"
	"testing"
	"time"

	"lockd/internal/metrics"
)

func TestBug10_TwoExpirerWorkersShutdownCleanly(t *testing.T) {
	service := NewService(NewRegistry(nil, 10, time.Second), NewBus(), &metrics.Metrics{}, "secret")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		service.RunExpirer(ctx, time.Hour, 2)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expirer shutdown did not complete")
	}
}
