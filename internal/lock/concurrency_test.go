package lock

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"lockd/internal/metrics"
)

// TestConcurrentListGetSnapshotIsolation reproduces the data race and response
// aliasing reported when many locks (each with an active wait queue) are read
// concurrently with List, paginated List slicing, and single-lock Get while the
// queues are being mutated. Under the old shared-buffer implementation the
// -race detector reports races on record.queueBuffer / Registry.listBuffer and
// serialized responses can contain empty or cross-contaminated queue entries.
func TestConcurrentListGetSnapshotIsolation(t *testing.T) {
	const numLocks = 40
	const numReaders = 16
	const iterations = 50

	registry := NewRegistry(nil, numLocks*2, 2*time.Second)
	service := NewService(registry, NewBus(), &metrics.Metrics{}, "secret")

	// Create many locks and seat a held holder on each so subsequent wait:true
	// acquires form a stable FIFO queue that snapshot/List must render.
	for i := 0; i < numLocks; i++ {
		ns := "ns"
		name := lockName(i)
		if _, err := service.Create(CreateOptions{
			Namespace: ns, Name: name, Reentrant: false, MaxDepth: 64, DefaultTTL: 30 * time.Second,
		}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if _, err := service.Acquire(context.Background(), ns, name, AcquireOptions{Holder: "owner"}); err != nil {
			t.Fatalf("acquire owner for %s: %v", name, err)
		}
		// Two blocked waiters per lock so each queue has length 2 with distinct holders.
		for w := 0; w < 2; w++ {
			holder := waiterHolder(i, w)
			go func() {
				_, _ = service.Acquire(context.Background(), ns, name, AcquireOptions{
					Holder: holder, Wait: true,
				})
			}()
		}
	}

	// Give the waiters time to enqueue.
	time.Sleep(100 * time.Millisecond)

	// Each entry in the queue must reference exactly this lock's waiters.
	expectedHolders := make(map[string]map[string]struct{}, numLocks)
	for i := 0; i < numLocks; i++ {
		set := map[string]struct{}{}
		set["owner"] = struct{}{}
		set[waiterHolder(i, 0)] = struct{}{}
		set[waiterHolder(i, 1)] = struct{}{}
		expectedHolders[lockName(i)] = set
	}

	var wg sync.WaitGroup
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for it := 0; it < iterations; it++ {
				// Full list (mirrors GET /locks).
				full := service.List("", "all")
				for _, view := range full {
					validateQueue(t, view, expectedHolders)
				}

				// Paginated slicing (mirrors offset/limit handling in listLocks).
				offset := (it + seed) % (numLocks + 1)
				limit := 7
				page := paginate(full, offset, limit)
				for _, view := range page {
					validateQueue(t, view, expectedHolders)
				}

				// Single-lock detail (mirrors GET /locks/{ns}/{name}).
				name := lockName((it + seed) % numLocks)
				view, err := service.Get("ns", name)
				if err != nil {
					t.Errorf("get %s: %v", name, err)
					continue
				}
				validateQueue(t, view, expectedHolders)

				// Simulate concurrent response encoding: the View and its Queue
				// slice must survive JSON marshalling with private backing storage.
				if _, err := json.Marshal(view); err != nil {
					t.Errorf("marshal view %s: %v", name, err)
				}
			}
		}(r)
	}
	wg.Wait()
}

func validateQueue(t *testing.T, view View, expected map[string]map[string]struct{}) {
	t.Helper()
	want, ok := expected[view.Name]
	if !ok {
		t.Errorf("view for unknown lock %q", view.Name)
		return
	}
	if view.QueueLength != len(view.Queue) {
		t.Errorf("queue length mismatch for %s: header=%d slice=%d", view.Name, view.QueueLength, len(view.Queue))
	}
	seen := make(map[string]int, len(view.Queue))
	for _, entry := range view.Queue {
		if entry.Holder == "" {
			t.Errorf("empty holder in queue of %s: %+v", view.Name, view.Queue)
			return
		}
		if _, allowed := want[entry.Holder]; !allowed {
			t.Errorf("cross-contaminated holder %q in queue of %s (owners=%v): %+v",
				entry.Holder, view.Name, want, view.Queue)
			return
		}
		seen[entry.Holder]++
	}
	for holder := range want {
		if holder == "owner" {
			continue // owner is not a waiter, it holds the lock.
		}
		if seen[holder] != 1 {
			t.Errorf("missing/duplicate waiter %q for %s: %+v", holder, view.Name, view.Queue)
			return
		}
	}
}

// paginate mirrors the offset/limit slicing in listLocks without mutating input.
func paginate(views []View, offset, limit int) []View {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(views) {
		return []View{}
	}
	views = views[offset:]
	if limit > 0 && limit < len(views) {
		views = views[:limit]
	}
	return views
}

func lockName(i int) string {
	return "lock-" + itoa(i)
}
func waiterHolder(i, w int) string {
	return "w-" + itoa(i) + "-" + itoa(w)
}

// itoa avoids pulling strconv just for small non-negative ints.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
