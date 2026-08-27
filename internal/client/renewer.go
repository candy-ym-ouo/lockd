package client

import (
	"context"
	"math/rand"
	"time"
)

func (c *Client) StartRenewer(parent context.Context, lease *Lease, onLost func(error)) context.CancelFunc {
	// Derive the renewer's lifecycle from the caller-provided parent, not from
	// any single request's context. A request context (e.g. the one passed to
	// Acquire) is short-lived; coupling the renewer to it would cancel the
	// renewal loop as soon as that request finished, leaving the lease to drift
	// idle. The returned cancel and parent cancellation both stop the loop.
	ctx, cancel := context.WithCancel(parent)
	go func() {
		failures := 0
		base := lease.TTL / 3
		if base < 100*time.Millisecond {
			base = 100 * time.Millisecond
		}
		for {
			jitter := 0.8 + rand.Float64()*0.4
			timer := time.NewTimer(time.Duration(float64(base) * jitter))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			err := c.Renew(ctx, lease)
			if err == nil {
				failures = 0
				continue
			}
			failures++
			if failures >= 3 {
				if onLost != nil {
					onLost(err)
				}
				return
			}
		}
	}()
	return cancel
}
