package lock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"lockd/internal/metrics"
)

func TestServiceAccessorsStatsAndErrorText(t *testing.T) {
	service := newTestService(t)
	if service.Registry() == nil || service.Bus() == nil || service.Metrics() == nil || service.StartedAt().IsZero() {
		t.Fatal("service accessors returned empty values")
	}
	if views := service.List("test", "idle"); len(views) != 1 {
		t.Fatalf("unexpected list: %#v", views)
	}
	locks, waiters, namespaces := service.Registry().Stats()
	if locks != 1 || waiters != 0 || namespaces["test"] != 1 {
		t.Fatalf("unexpected stats: %d %d %#v", locks, waiters, namespaces)
	}
	if !strings.Contains(ErrLocked.Error(), "10004") {
		t.Fatalf("error text omitted code: %s", ErrLocked)
	}
	if renewalInterval(150*time.Millisecond) != 100*time.Millisecond {
		t.Fatal("minimum renewal interval was not applied")
	}
}

func TestDeleteRulesAndWaiterCancellation(t *testing.T) {
	service := newTestService(t)
	stale, _ := service.registry.get("test", "resource")
	owner, _ := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "owner"})
	waitResult := make(chan error, 1)
	go func() {
		_, err := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "waiter", Wait: true})
		waitResult <- err
	}()
	waitForQueue(t, service, 1)
	if err := service.Delete("test", "resource", ""); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected held delete rejection, got %v", err)
	}
	if err := service.Delete("test", "resource", "wrong"); !errors.Is(err, ErrForceUnauthorized) {
		t.Fatalf("expected force rejection, got %v", err)
	}
	if err := service.Delete("test", "resource", "secret"); err != nil {
		t.Fatal(err)
	}
	if err := <-waitResult; !errors.Is(err, ErrNotFound) {
		t.Fatalf("waiter was not cancelled by purge: %v", err)
	}
	if _, err := service.Get("test", "resource"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted lock still exists: %v, old token %s", err, owner.Token)
	}
	stale.mu.Lock()
	deleted := stale.deleted
	stale.mu.Unlock()
	if !deleted {
		t.Fatal("removed record was not marked deleted for stale callers")
	}
}

func TestWatchReceivesRelease(t *testing.T) {
	service := newTestService(t)
	lease, _ := service.Acquire(context.Background(), "test", "resource", AcquireOptions{Holder: "owner"})
	events := make(chan Event, 1)
	errorsCh := make(chan error, 1)
	go func() {
		event, err := service.Watch(context.Background(), "test", "resource")
		events <- event
		errorsCh <- err
	}()
	time.Sleep(5 * time.Millisecond)
	if _, err := service.Release("test", "resource", lease.Token); err != nil {
		t.Fatal(err)
	}
	if err := <-errorsCh; err != nil {
		t.Fatal(err)
	}
	if event := <-events; event.Event != EventReleased || event.Reason != "release" {
		t.Fatalf("unexpected watch event: %#v", event)
	}
}

func TestRunExpirerAndAutoCleanup(t *testing.T) {
	registry := NewRegistry(nil, 10, time.Second)
	service := NewService(registry, NewBus(), &metrics.Metrics{}, "")
	_, err := service.Create(CreateOptions{
		Namespace: "n", Name: "lease", Reentrant: true, DefaultTTL: time.Second, AutoCleanup: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = service.Acquire(context.Background(), "n", "lease", AcquireOptions{Holder: "owner"})
	item, _ := registry.get("n", "lease")
	item.mu.Lock()
	item.expiresAt = time.Now().Add(-time.Millisecond)
	item.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	go service.RunExpirer(ctx, time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, lookupErr := service.Get("n", "lease")
		if errors.Is(lookupErr, ErrNotFound) {
			cancel()
			item.mu.Lock()
			deleted := item.deleted
			item.mu.Unlock()
			if !deleted {
				t.Fatal("auto-cleaned record was not marked deleted")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	t.Fatal("expirer did not expire and clean up lock")
}

func TestDepthTTLAndNonReentrantValidation(t *testing.T) {
	registry := NewRegistry(nil, 10, time.Second)
	service := NewService(registry, nil, nil, "")
	_, err := service.Create(CreateOptions{Namespace: "n", Name: "depth", Reentrant: true, MaxDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	lease, _ := service.Acquire(context.Background(), "n", "depth", AcquireOptions{Holder: "same"})
	if _, err := service.Acquire(context.Background(), "n", "depth", AcquireOptions{Holder: "same"}); err == nil {
		t.Fatal("expected maximum depth error")
	}
	if _, err := service.Renew("n", "depth", lease.Token, 500*time.Millisecond); err == nil {
		t.Fatal("expected ttl validation error")
	}
	_, _ = service.Release("n", "depth", lease.Token)
	if _, err := service.Renew("n", "depth", lease.Token, 0); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("expected not-held renew error, got %v", err)
	}

	_, _ = service.Create(CreateOptions{Namespace: "n", Name: "plain", Reentrant: false})
	plain, _ := service.Acquire(context.Background(), "n", "plain", AcquireOptions{Holder: "same"})
	if _, err := service.Acquire(context.Background(), "n", "plain", AcquireOptions{Holder: "same"}); !errors.Is(err, ErrLocked) {
		t.Fatalf("non-reentrant lock should reject same holder: %v", err)
	}
	_, _ = service.Release("n", "plain", plain.Token)
}

func TestBusDropsSlowSubscriberAndQueueCancelAll(t *testing.T) {
	bus := NewBus()
	channel, cancel := bus.Subscribe(1)
	defer cancel()
	if delivered := bus.Broadcast(Event{Event: EventHeld}); delivered != 1 {
		t.Fatalf("first broadcast delivered to %d subscribers", delivered)
	}
	if delivered := bus.Broadcast(Event{Event: EventReleased}); delivered != 0 {
		t.Fatalf("full subscriber should be skipped, got %d", delivered)
	}
	<-channel

	first := newWaiter(1, AcquireOptions{Holder: "one"}, time.Now())
	second := newWaiter(2, AcquireOptions{Holder: "two"}, time.Now())
	queue := waiterQueue{items: []*waiter{first, second}}
	queue.cancelAll(ErrNotFound)
	if queue.activeLength() != 0 || !errors.Is((<-first.done).err, ErrNotFound) || !errors.Is((<-second.done).err, ErrNotFound) {
		t.Fatal("cancelAll did not notify every waiter")
	}
}
