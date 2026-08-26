package lock

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

const (
	MinTTL     = time.Second
	MaxTTL     = 10 * time.Minute
	DefaultTTL = 30 * time.Second
)

func normalizeTTL(requested, fallback time.Duration) (time.Duration, error) {
	if fallback <= 0 {
		fallback = DefaultTTL
	}
	if requested == 0 {
		requested = fallback
	}
	if requested < MinTTL || requested > MaxTTL {
		return 0, &Error{Code: CodeInvalid, Message: "ttl must be between 1s and 10m"}
	}
	return requested, nil
}
func leaseExpiry(now time.Time, ttl time.Duration) time.Time {
	return now.Add(ttl).UTC()
}
func leaseExpired(expiresAt, now time.Time) bool {
	return !expiresAt.IsZero() && !now.Before(expiresAt)
}
func newToken() (string, error) {
	buffer := make([]byte, 18)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return "tk_" + hex.EncodeToString(buffer), nil
}
func renewalInterval(ttl time.Duration) time.Duration {
	interval := ttl / 3
	if interval < 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	return interval
}
func assignLease(r *record, holder, token string, ttl time.Duration, now time.Time) AcquireResult {
	r.holder = holder
	r.token = token
	r.depth = 1
	r.expiresAt = leaseExpiry(now, ttl)
	r.version++
	return AcquireResult{
		Token:     token,
		Holder:    holder,
		Depth:     1,
		ExpiresAt: r.expiresAt,
	}
}
func clearLease(r *record, now time.Time, rememberRelease bool) {
	if r.token != "" && rememberRelease {
		// FIX: only voluntary releases are idempotent; stolen/expired tokens stay invalid.
		if len(r.releasedToken) >= 128 {
			r.releasedToken = make(map[string]struct{})
		}
		r.releasedToken[r.token] = struct{}{}
	}
	r.holder = ""
	r.token = ""
	r.depth = 0
	r.expiresAt = time.Time{}
	r.lastIdleAt = now.UTC()
	r.version++
}
