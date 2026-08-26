package lock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"lockd/internal/metrics"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	registry := NewRegistry(nil, 100, 2*time.Second)
	service := NewService(registry, NewBus(), &metrics.Metrics{}, "secret")
	_, err := service.Create(CreateOptions{
		Namespace: "test", Name: "resource", Reentrant: true, MaxDepth: 4, DefaultTTL: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestAcquireReenterRenewAndRelease(t *testing.T) {
	service := newTestService(t)
	first, err := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "holder-a"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "holder-a"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Token != first.Token || second.Depth != 2 || !second.Reentered {
		t.Fatalf("unexpected reentry: %#v", second)
	}
	if _, err := service.Renew("test", "resource", first.Token, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	partial, err := service.Release("test", "resource", first.Token)
	if err != nil || partial.Released || partial.Depth != 1 {
		t.Fatalf("unexpected partial release: %#v, %v", partial, err)
	}
	final, err := service.Release("test", "resource", first.Token)
	if err != nil || !final.Released || final.Depth != 0 {
		t.Fatalf("unexpected final release: %#v, %v", final, err)
	}
	if _, err := service.Release("test", "resource", first.Token); err != nil {
		t.Fatalf("release should be idempotent: %v", err)
	}
}

func TestTryAcquireAndInvalidToken(t *testing.T) {
	service := newTestService(t)
	lease, err := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "holder-a"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "holder-b"})
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("expected locked, got %v", err)
	}
	if _, err := service.Renew("test", "resource", "bad-token", 0); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("expected token error, got %v", err)
	}
	if _, err := service.Release("test", "resource", lease.Token); err != nil {
		t.Fatal(err)
	}
}

func TestFIFOQueue(t *testing.T) {
	service := newTestService(t)
	owner, err := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan AcquireResult, 2)
	errorsCh := make(chan error, 2)
	acquire := func(holder string) {
		result, acquireErr := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: holder, Wait: true})
		results <- result
		errorsCh <- acquireErr
	}
	go acquire("first")
	waitForQueue(t, service, 1)
	go acquire("second")
	waitForQueue(t, service, 2)
	if _, err := service.Release("test", "resource", owner.Token); err != nil {
		t.Fatal(err)
	}
	first := <-results
	if err := <-errorsCh; err != nil {
		t.Fatal(err)
	}
	if first.Holder != "first" {
		t.Fatalf("FIFO violation: first grant was %s", first.Holder)
	}
	if duplicate, err := service.Release("test", "resource", owner.Token); err != nil || duplicate.Released {
		t.Fatalf("old token retry must be idempotent after handoff: %#v, %v", duplicate, err)
	}
	if _, err := service.Release("test", "resource", first.Token); err != nil {
		t.Fatal(err)
	}
	second := <-results
	if err := <-errorsCh; err != nil {
		t.Fatal(err)
	}
	if second.Holder != "second" {
		t.Fatalf("FIFO violation: second grant was %s", second.Holder)
	}
}

func TestWaitCancellationIsRemoved(t *testing.T) {
	service := newTestService(t)
	owner, _ := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "owner"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := service.Acquire(ctx, "test", "resource", AcquireOptions{Holder: "waiter", Wait: true})
	if !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("expected timeout, got %v", err)
	}
	view, _ := service.Get("test", "resource")
	if view.QueueLength != 0 {
		t.Fatalf("cancelled waiter remains queued: %d", view.QueueLength)
	}
	_, _ = service.Release("test", "resource", owner.Token)
}

func TestExpiryWatchAndSteal(t *testing.T) {
	service := newTestService(t)
	lease, _ := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "old"})
	if _, err := service.Steal("test", "resource", "wrong", "ops", 0); !errors.Is(err, ErrForceUnauthorized) {
		t.Fatalf("expected force-token error, got %v", err)
	}
	stolen, err := service.Steal("test", "resource", "secret", "ops", 0)
	if err != nil || stolen.Token == lease.Token {
		t.Fatalf("unexpected steal result %#v, %v", stolen, err)
	}
	if _, err := service.Release("test", "resource", lease.Token); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("old token should be invalid: %v", err)
	}

	item, _ := service.registry.get("test", "resource")
	item.mu.Lock()
	item.expiresAt = time.Now().Add(-time.Millisecond)
	item.mu.Unlock()
	service.expirePass(time.Now())
	view, _ := service.Get("test", "resource")
	if view.State != "idle" {
		t.Fatalf("expected expiry to make lock idle: %#v", view)
	}
	event, err := service.Watch(context.Background(), "test", "resource")
	if err != nil || event.Reason != "idle" {
		t.Fatalf("idle watch did not return immediately: %#v, %v", event, err)
	}
}

func TestConcurrentTryAcquireHasSingleWinner(t *testing.T) {
	service := newTestService(t)
	var wg sync.WaitGroup
	winners := make(chan AcquireResult, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			result, err := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: string(rune('a' + index))})
			if err == nil {
				winners <- result
			} else if !errors.Is(err, ErrLocked) {
				t.Errorf("unexpected acquire error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	close(winners)
	if count := len(winners); count != 1 {
		t.Fatalf("expected one winner, got %d", count)
	}
}

func TestCancelledAcquireDoesNotTakeIdleLock(t *testing.T) {
	service := newTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Acquire(ctx, "test", "resource", AcquireOptions{Holder: "gone"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire returned %v", err)
	}
	view, err := service.Get("test", "resource")
	if err != nil || view.State != "idle" {
		t.Fatalf("cancelled caller left a lease behind: %#v, %v", view, err)
	}
}

func TestExpiredLeaseIsHandledBeforeBackgroundScan(t *testing.T) {
	expire := func(service *Service) {
		item, _ := service.registry.get("test", "resource")
		item.mu.Lock()
		item.expiresAt = time.Now().Add(-time.Millisecond)
		item.mu.Unlock()
	}
	t.Run("release rejects expired token", func(t *testing.T) {
		service := newTestService(t)
		lease, _ := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "expired"})
		expire(service)
		if _, err := service.Release("test", "resource", lease.Token); !errors.Is(err, ErrNotHeld) {
			t.Fatalf("expired token release returned %v", err)
		}
	})
	t.Run("delete accepts expired lock", func(t *testing.T) {
		service := newTestService(t)
		_, _ = service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "expired"})
		expire(service)
		if err := service.Delete("test", "resource", ""); err != nil {
			t.Fatalf("expired lock could not be deleted: %v", err)
		}
	})
}

func waitForQueue(t *testing.T, service *Service, expected int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		view, _ := service.Get("test", "resource")
		if view.QueueLength == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue did not reach %d", expected)
}
