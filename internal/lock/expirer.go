package lock

import (
	"context"
	"time"
)

func (s *Service) RunExpirer(ctx context.Context, interval time.Duration) {
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
			return
		}
	}
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
