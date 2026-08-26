package client

import (
	"context"
	"math/rand"
	"time"
)

func (c *Client) StartRenewer(parent context.Context, lease *Lease, onLost func(error)) context.CancelFunc {
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
