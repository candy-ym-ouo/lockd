package lock

import "time"

func tryReenter(r *record, holder string, ttl time.Duration, now time.Time) (AcquireResult, bool, error) {
	if r.holder != holder || r.token == "" {
		return AcquireResult{}, false, nil
	}
	if !r.reentrant {
		return AcquireResult{}, false, nil
	}
	if r.depth >= r.maxDepth {
		return AcquireResult{}, true, &Error{Code: CodeInvalid, Message: "maximum reentrant depth reached"}
	}
	r.depth++
	r.expiresAt = leaseExpiry(now, ttl)
	r.version++
	return AcquireResult{
		Token:     r.token,
		Holder:    r.holder,
		Depth:     r.depth,
		ExpiresAt: r.expiresAt,
		Reentered: true,
	}, true, nil
}
func decreaseDepth(r *record, now time.Time) ReleaseResult {
	if r.depth > 1 {
		r.depth--
		r.expiresAt = leaseExpiry(now, r.defaultTTL)
		r.version++
		return ReleaseResult{Released: false, Depth: r.depth}
	}
	return ReleaseResult{Released: true, Depth: 0}
}
