package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTransport routes a request to one of the canned payloads based on the path suffix.
func fakeTransport(renew, release, acquire []byte) roundTripFunc {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body []byte
		switch {
		case strings.HasSuffix(r.URL.Path, "/renew"):
			body = renew
		case strings.HasSuffix(r.URL.Path, "/release"):
			body = release
		default:
			body = acquire
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})
}

// TestConcurrentCacheAccessNoPanic hammers the shared lease cache from many
// goroutines on overlapping lock keys: Acquire writes, StartRenewer writes and
// (on failure) deletes, removeLease deletes, ActiveLeases reads. Without the
// Client mutex this is a fatal "concurrent map writes" runtime panic.
func TestConcurrentCacheAccessNoPanic(t *testing.T) {
	acquire := []byte(`{"code":0,"msg":"ok","data":{"token":"tk","holder":"h","depth":1,"expires_at":"2030-01-01T00:00:00Z"}}`)
	renew := []byte(`{"code":0,"msg":"ok","data":{"expires_at":"2030-01-01T00:00:00Z"}}`)
	release := []byte(`{"code":0,"msg":"ok","data":{"released":true,"depth":0}}`)
	client := New("http://lockd.invalid")
	client.SetHTTPClient(&http.Client{Transport: fakeTransport(renew, release, acquire)})

	const workers = 200
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		name := fmt.Sprintf("n%d", i%8)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.Acquire(context.Background(), "ns", name, "h", AcquireOptions{})
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			cancel := client.StartRenewer(context.Background(),
				&Lease{Namespace: "ns", Name: name, Holder: "h", Token: "tk", TTL: 30 * time.Second}, nil)
			time.Sleep(time.Millisecond)
			cancel()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			client.removeLease("ns", name)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.ActiveLeases()
		}()
	}
	wg.Wait()
}

// TestSharedLeaseFieldUpdatesRace stresses Renew writing ExpiresAt and Release
// writing Depth on a shared *Lease while external goroutines read those fields
// via the safe accessors. Run with -race; the per-lease mutex keeps it clean.
func TestSharedLeaseFieldUpdatesRace(t *testing.T) {
	renew := []byte(`{"code":0,"msg":"ok","data":{"expires_at":"2030-01-01T00:00:00Z"}}`)
	release := []byte(`{"code":0,"msg":"ok","data":{"released":true,"depth":0}}`)
	client := New("http://lockd.invalid")
	client.SetHTTPClient(&http.Client{Transport: fakeTransport(renew, release, nil)})

	lease := &Lease{Namespace: "ns", Name: "n", Holder: "h", Token: "tk", TTL: 30 * time.Second, ExpiresAt: time.Now().Add(30 * time.Second)}
	client.mu.Lock()
	client.leases[lockKey("ns", "n")] = lease
	client.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = client.Renew(context.Background(), lease) }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.Release(context.Background(), lease)
			// re-register so concurrent deletes balance against writers
			client.mu.Lock()
			client.leases[lockKey("ns", "n")] = lease
			client.mu.Unlock()
		}()
		wg.Add(1)
		go func() { defer wg.Done(); _ = client.ActiveLeases() }()
		wg.Add(1)
		go func() { defer wg.Done(); _ = lease.Expiry(); _ = lease.CurrentDepth() }()
	}
	wg.Wait()
}
