package lock

import (
	"context"
	"sync"
	"time"
)

func (s *Service) RunExpirer(ctx context.Context, interval time.Duration, workerOption ...int) {
	workers := 1
	if len(workerOption) > 0 && workerOption[0] > 0 {
		workers = workerOption[0]
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			s.expirePass(now.UTC())
		case <-ctx.Done():
			s.stopExpirerWorkers(workers)
			return
		}
	}
}
func (s *Service) stopExpirerWorkers(workers int) {
	// FIX: register one WaitGroup entry per partition we actually launch. The
	// previous code subtracted 1 from the partition count when it exceeded 1,
	// so with the default two workers it Add(1)'d while two goroutines each
	// Done()'d — a guaranteed "sync: negative WaitGroup counter" on SIGTERM.
	// Empty partitions still launch a goroutine, so the counter must equal
	// len(partitions) regardless of worker count.
	partitions := s.registry.workerPartitions(workers)
	var wait sync.WaitGroup
	wait.Add(len(partitions))
	for _, partition := range partitions {
		partition := partition
		go func() {
			defer wait.Done()
			for _, item := range partition {
				item.mu.Lock()
				item.queue.cancelAll(context.Canceled)
				item.mu.Unlock()
			}
		}()
	}
	wait.Wait()
}
func (s *Service) expirePass(now time.Time) {
	for _, item := range s.registry.allRecords() {
		item.mu.Lock()
		if item.deleted {
			item.mu.Unlock()
			continue
		}
		s.expireLocked(item, now)
		cleanup := item.token == "" &&
			item.autoCleanup > 0 &&
			item.queue.activeLength() == 0 &&
			now.Sub(item.lastIdleAt) >= item.autoCleanup
		if cleanup {
			// FIX: stale requests that already hold this pointer must see deletion.
			item.deleted = true
			s.registry.remove(item)
		}
		item.mu.Unlock()
		if cleanup {
			s.emit(item, EventPurged, "auto_cleanup", now)
		}
	}
}
