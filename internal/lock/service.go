package lock

import (
	"context"
	"lockd/internal/metrics"
	"sync/atomic"
	"time"
)

type Service struct {
	registry *Registry
	bus      *Bus
	metrics  *metrics.Metrics
	force    string
	seq      atomic.Uint64
	eventSeq atomic.Uint64
	started  time.Time
}

func NewService(registry *Registry, bus *Bus, metricSet *metrics.Metrics, forceToken string) *Service {
	if bus == nil {
		bus = NewBus()
	}
	if metricSet == nil {
		metricSet = &metrics.Metrics{}
	}
	return &Service{
		registry: registry,
		bus:      bus,
		metrics:  metricSet,
		force:    forceToken,
		started:  time.Now().UTC(),
	}
}
func (s *Service) Registry() *Registry                        { return s.registry }
func (s *Service) Bus() *Bus                                  { return s.bus }
func (s *Service) Metrics() *metrics.Metrics                  { return s.metrics }
func (s *Service) StartedAt() time.Time                       { return s.started }
func (s *Service) Create(options CreateOptions) (View, error) { return s.registry.Create(options) }
func (s *Service) Get(namespace, name string) (View, error)   { return s.registry.Get(namespace, name) }
func (s *Service) List(namespace, state string) []View        { return s.registry.List(namespace, state) }

func (s *Service) Acquire(ctx context.Context, namespace, name string, options AcquireOptions) (AcquireResult, error) {
	if options.Holder == "" || len(options.Holder) > 64 {
		return AcquireResult{}, &Error{Code: CodeInvalid, Message: "holder length must be 1 to 64"}
	}
	if len(options.RequestID) > 64 {
		return AcquireResult{}, &Error{Code: CodeInvalid, Message: "request_id is too long"}
	}
	// FIX: never create an orphan lease for a caller that has already disconnected.
	if err := ctx.Err(); err != nil {
		return AcquireResult{}, err
	}
	item, err := s.registry.get(namespace, name)
	if err != nil {
		return AcquireResult{}, err
	}
	now := time.Now().UTC()
	item.mu.Lock()
	// FIX: reject a record removed after registry.get returned it.
	if item.deleted {
		item.mu.Unlock()
		return AcquireResult{}, ErrNotFound
	}
	ttl, err := normalizeTTL(options.TTL, item.defaultTTL)
	if err != nil {
		item.mu.Unlock()
		return AcquireResult{}, err
	}
	options.TTL = ttl
	s.expireLocked(item, now)
	if item.token == "" && item.queue.activeLength() == 0 {
		result, grantErr := s.assignLocked(item, options.Holder, ttl, now)
		item.mu.Unlock()
		if grantErr != nil {
			return AcquireResult{}, grantErr
		}
		s.metrics.IncAcquire()
		s.emit(item, EventHeld, "acquire", now)
		return result, nil
	}
	if result, handled, reenterErr := tryReenter(item, options.Holder, ttl, now); handled {
		item.mu.Unlock()
		if reenterErr == nil {
			s.metrics.IncAcquire()
			s.emit(item, EventHeld, "reenter", now)
		}
		return result, reenterErr
	}
	if !options.Wait {
		item.mu.Unlock()
		s.metrics.IncAcquireLocked()
		return AcquireResult{}, ErrLocked
	}
	w := newWaiter(s.seq.Add(1), options, now)
	item.queue.push(w)
	item.version++
	item.mu.Unlock()
	select {
	case outcome := <-w.done:
		if outcome.err == nil {
			s.metrics.IncAcquire()
		}
		return outcome.result, outcome.err
	case <-ctx.Done():
		item.mu.Lock()
		if w.granted {
			result := w.result
			item.mu.Unlock()
			s.metrics.IncAcquire()
			return result, nil
		}
		w.cancel()
		item.queue.remove(w)
		item.version++
		item.mu.Unlock()
		if ctx.Err() == context.DeadlineExceeded {
			return AcquireResult{}, ErrWaitTimeout
		}
		return AcquireResult{}, ctx.Err()
	}
}
func (s *Service) assignLocked(item *record, holder string, ttl time.Duration, now time.Time) (AcquireResult, error) {
	token, err := newToken()
	if err != nil {
		return AcquireResult{}, &Error{Code: CodeInternal, Message: "could not create lease token"}
	}
	return assignLease(item, holder, token, ttl, now), nil
}

func (s *Service) Renew(namespace, name, token string, requestedTTL time.Duration) (time.Time, error) {
	item, err := s.registry.get(namespace, name)
	if err != nil {
		s.metrics.IncRenewFail()
		return time.Time{}, err
	}
	now := time.Now().UTC()
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.deleted {
		s.metrics.IncRenewFail()
		return time.Time{}, ErrNotFound
	}
	if item.token == "" || s.expireLocked(item, now) != "" {
		s.metrics.IncRenewFail()
		return time.Time{}, ErrNotHeld
	}
	if token == "" || token != item.token {
		s.metrics.IncRenewFail()
		return time.Time{}, ErrTokenInvalid
	}
	ttl, err := normalizeTTL(requestedTTL, item.defaultTTL)
	if err != nil {
		s.metrics.IncRenewFail()
		return time.Time{}, err
	}
	item.expiresAt = leaseExpiry(now, ttl)
	item.version++
	s.metrics.IncRenew()
	return item.expiresAt, nil
}

func (s *Service) Release(namespace, name, token string) (ReleaseResult, error) {
	item, err := s.registry.get(namespace, name)
	if err != nil {
		return ReleaseResult{}, err
	}
	now := time.Now().UTC()
	item.mu.Lock()
	if item.deleted {
		item.mu.Unlock()
		return ReleaseResult{}, ErrNotFound
	}
	// FIX: a retried release remains idempotent even after the lock was handed on.
	if _, releasedBefore := item.releasedToken[token]; releasedBefore {
		item.mu.Unlock()
		return ReleaseResult{Released: false}, nil
	}
	// FIX: an expired token is invalid even before the background scan observes it.
	if expiredToken := s.expireLocked(item, now); expiredToken != "" {
		item.mu.Unlock()
		if token == expiredToken {
			return ReleaseResult{}, ErrNotHeld
		}
		return ReleaseResult{}, ErrTokenInvalid
	}
	if item.token == "" {
		item.mu.Unlock()
		return ReleaseResult{}, ErrNotHeld
	}
	if token == "" || token != item.token {
		item.mu.Unlock()
		return ReleaseResult{}, ErrTokenInvalid
	}
	result := decreaseDepth(item, now)
	if !result.Released {
		item.mu.Unlock()
		return result, nil
	}
	clearLease(item, now, true)
	next, grantEvent := s.grantNextLocked(item, now)
	result.NextHolder = next
	item.mu.Unlock()
	s.emit(item, EventReleased, "release", now)
	if grantEvent {
		s.emit(item, EventHeld, "queue", now)
	}
	return result, nil
}
func (s *Service) grantNextLocked(item *record, now time.Time) (string, bool) {
	for {
		w := item.queue.pop()
		if w == nil {
			return "", false
		}
		result, err := s.assignLocked(item, w.holder, w.ttl, now)
		if err != nil {
			w.done <- acquireOutcome{err: err}
			continue
		}
		w.grant(result)
		return w.holder, true
	}
}

func (s *Service) Watch(ctx context.Context, namespace, name string) (Event, error) {
	channel, cancel := s.bus.Subscribe(8)
	defer cancel()
	view, err := s.registry.Get(namespace, name)
	if err != nil {
		return Event{}, err
	}
	if view.State == "idle" {
		return Event{Lock: view.FullName, Namespace: namespace, Event: EventReleased, Reason: "idle", At: time.Now().UTC()}, nil
	}
	for {
		select {
		case event := <-channel:
			if event.Lock == view.FullName && event.Event != EventHeld {
				return event, nil
			}
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return Event{}, ErrWaitTimeout
			}
			return Event{}, ctx.Err()
		}
	}
}

func (s *Service) Steal(namespace, name, forceToken, holder string, ttl time.Duration) (AcquireResult, error) {
	if s.force == "" || forceToken != s.force {
		return AcquireResult{}, ErrForceUnauthorized
	}
	if holder == "" || len(holder) > 64 {
		return AcquireResult{}, ErrInvalid
	}
	item, err := s.registry.get(namespace, name)
	if err != nil {
		return AcquireResult{}, err
	}
	now := time.Now().UTC()
	item.mu.Lock()
	if item.deleted {
		item.mu.Unlock()
		return AcquireResult{}, ErrNotFound
	}
	ttl, err = normalizeTTL(ttl, item.defaultTTL)
	if err != nil {
		item.mu.Unlock()
		return AcquireResult{}, err
	}
	clearLease(item, now, false)
	result, err := s.assignLocked(item, holder, ttl, now)
	item.mu.Unlock()
	if err != nil {
		return AcquireResult{}, err
	}
	s.metrics.IncSteal()
	s.emit(item, EventStolen, "steal", now)
	return result, nil
}

func (s *Service) Delete(namespace, name, forceToken string) error {
	item, err := s.registry.get(namespace, name)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	item.mu.Lock()
	force := s.force != "" && forceToken == s.force
	if forceToken != "" && !force {
		item.mu.Unlock()
		return ErrForceUnauthorized
	}
	// FIX: do not reject deletion solely because the expirer has not scanned an expired lease yet.
	s.expireLocked(item, now)
	if item.token != "" && !force {
		item.mu.Unlock()
		return ErrLocked
	}
	// FIX: mark deleted while holding the record lock before removing the map entry.
	item.deleted = true
	item.queue.cancelAll(ErrNotFound)
	clearLease(item, now, false)
	removed := s.registry.remove(item)
	item.mu.Unlock()
	if !removed {
		return ErrNotFound
	}
	s.emit(item, EventPurged, "purge", now)
	return nil
}
func (s *Service) expireLocked(item *record, now time.Time) string {
	if item.token == "" || !leaseExpired(item.expiresAt, now) {
		return ""
	}
	expiredToken := item.token
	clearLease(item, now, false)
	_, granted := s.grantNextLocked(item, now)
	s.metrics.IncExpiry()
	s.emit(item, EventExpired, "expire", now)
	if granted {
		s.emit(item, EventHeld, "queue", now)
	}
	return expiredToken
}
func (s *Service) emit(item *record, eventType, reason string, now time.Time) {
	event := Event{
		Lock:      item.fullName(),
		Namespace: item.namespace,
		Event:     eventType,
		Reason:    reason,
		At:        now.UTC(),
		Seq:       s.eventSeq.Add(1),
	}
	if s.bus.Broadcast(event) > 0 {
		s.metrics.IncNotify()
	}
}
