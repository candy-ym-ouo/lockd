package lock

import "time"

type waiter struct {
	seq          uint64
	holder       string
	requestID    string
	ttl          time.Duration
	waitingSince time.Time
	done         chan acquireOutcome
	cancelled    bool
	granted      bool
	result       AcquireResult
}
type acquireOutcome struct {
	result AcquireResult
	err    error
}

func newWaiter(seq uint64, options AcquireOptions, now time.Time) *waiter {
	return &waiter{
		seq:          seq,
		holder:       options.Holder,
		requestID:    options.RequestID,
		ttl:          options.TTL,
		waitingSince: now.UTC(),
		done:         make(chan acquireOutcome, 1),
	}
}
func (w *waiter) view() WaiterView {
	return WaiterView{
		Seq:          w.seq,
		Holder:       w.holder,
		RequestID:    w.requestID,
		WaitingSince: w.waitingSince,
	}
}
func (w *waiter) grant(result AcquireResult) {
	w.granted = true
	w.result = result
	w.done <- acquireOutcome{result: result}
}
func (w *waiter) cancel() {
	w.cancelled = true
}
