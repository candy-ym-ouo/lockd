package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Error struct {
	Code int
	Msg  string
}

func (e *Error) Error() string { return fmt.Sprintf("lockd: %d %s", e.Code, e.Msg) }

type responseEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	retries    int
	// mu guards the leases map only. It is never held across a network call
	// (c.do) or a user callback, so those cannot block one another. Per-lease
	// field updates (ExpiresAt/Depth) use the Lease's own mutex, so they do
	// not contend with this lock and stay race-free even for external readers.
	mu     sync.Mutex
	leases map[string]*Lease
}

// Lease is a client-side view of a held lock. Its mutable fields (ExpiresAt,
// Depth) are written by Renew/Release from background goroutines (the renewer)
// and must be read concurrently-safe: use the Expiry/CurrentDepth accessors
// rather than touching the fields directly. Each Lease carries its own mutex
// (independent of the Client's map lock) so field updates never contend with
// the lease cache and vice versa.
type Lease struct {
	Namespace string        `json:"namespace"`
	Name      string        `json:"name"`
	Holder    string        `json:"holder"`
	Token     string        `json:"token"`
	Depth     int           `json:"depth"`
	ExpiresAt time.Time     `json:"expires_at"`
	TTL       time.Duration `json:"-"`

	mu sync.Mutex
}

// Expiry returns the latest known server-side expiry time, safe for concurrent
// access while a renewer is running.
func (l *Lease) Expiry() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ExpiresAt
}

// CurrentDepth returns the latest known reentrant depth, safe for concurrent
// access while Release is running.
func (l *Lease) CurrentDepth() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Depth
}

// setExpiry updates ExpiresAt under the lease lock.
func (l *Lease) setExpiry(t time.Time) {
	l.mu.Lock()
	l.ExpiresAt = t
	l.mu.Unlock()
}

// setDepth updates Depth under the lease lock.
func (l *Lease) setDepth(d int) {
	l.mu.Lock()
	l.Depth = d
	l.mu.Unlock()
}

type AcquireOptions struct {
	TTL         time.Duration
	Wait        bool
	WaitTimeout time.Duration
	RequestID   string
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		// FIX: accommodate the server's documented 10 minute acquire ceiling.
		httpClient: &http.Client{Timeout: 11 * time.Minute},
		retries:    3,
		leases:     make(map[string]*Lease),
	}
}
func (c *Client) SetHTTPClient(httpClient *http.Client) {
	if httpClient != nil {
		c.httpClient = httpClient
	}
}
func (c *Client) Create(ctx context.Context, namespace, name string, reentrant bool, ttl time.Duration) error {
	body := map[string]any{"namespace": namespace, "name": name, "reentrant": reentrant, "ttl": ttl.String()}
	return c.do(ctx, http.MethodPost, "/api/v1/locks", body, "", nil)
}
func (c *Client) List(ctx context.Context, namespace string) ([]map[string]any, error) {
	path := "/api/v1/locks"
	if namespace != "" {
		path += "?namespace=" + url.QueryEscape(namespace)
	}
	var result []map[string]any
	err := c.do(ctx, http.MethodGet, path, nil, "", &result)
	return result, err
}
func (c *Client) TryAcquire(ctx context.Context, namespace, name, holder string, options AcquireOptions) (*Lease, error) {
	options.Wait = false
	return c.Acquire(ctx, namespace, name, holder, options)
}
func (c *Client) Acquire(ctx context.Context, namespace, name, holder string, options AcquireOptions) (*Lease, error) {
	body := map[string]any{"holder": holder, "request_id": options.RequestID, "wait": options.Wait}
	if options.TTL > 0 {
		body["ttl"] = options.TTL.String()
	}
	if options.WaitTimeout > 0 {
		body["wait_timeout"] = options.WaitTimeout.String()
	}
	var response struct {
		Token     string    `json:"token"`
		Holder    string    `json:"holder"`
		Depth     int       `json:"depth"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	err := c.do(ctx, http.MethodPost, lockPath(namespace, name)+"/acquire", body, "", &response)
	if err != nil {
		return nil, err
	}
	// FIX: derive an omitted TTL from the server response instead of assuming its configuration is 30s.
	ttl := effectiveTTL(options.TTL, response.ExpiresAt)
	lease := &Lease{
		Namespace: namespace, Name: name, Holder: holder, Token: response.Token,
		Depth: response.Depth, ExpiresAt: response.ExpiresAt, TTL: ttl,
	}
	c.mu.Lock()
	c.leases[lockKey(namespace, name)] = lease
	c.mu.Unlock()
	return lease, nil
}
func (c *Client) Renew(ctx context.Context, lease *Lease) error {
	if lease == nil {
		return errors.New("lockd: nil lease")
	}
	body := map[string]any{"token": lease.Token, "ttl": lease.TTL.String()}
	var response struct {
		ExpiresAt time.Time `json:"expires_at"`
	}
	// Network call is outside any lock so it cannot block or be blocked by
	// map/field updates or by other goroutines' callbacks.
	err := c.do(ctx, http.MethodPost, lockPath(lease.Namespace, lease.Name)+"/renew", body, "", &response)
	if err == nil {
		// Field update goes through the per-lease lock (not the map lock), so a
		// concurrent reader using Expiry() stays race-free without touching the
		// lease cache.
		lease.setExpiry(response.ExpiresAt)
	}
	return err
}
func (c *Client) Release(ctx context.Context, lease *Lease) error {
	if lease == nil {
		return errors.New("lockd: nil lease")
	}
	var response struct {
		Released bool `json:"released"`
		Depth    int  `json:"depth"`
	}
	err := c.do(ctx, http.MethodPost, lockPath(lease.Namespace, lease.Name)+"/release", map[string]string{"token": lease.Token}, "", &response)
	if err != nil {
		return err
	}
	lease.setDepth(response.Depth)
	if response.Released || response.Depth == 0 {
		c.removeLease(lease.Namespace, lease.Name)
	}
	return nil
}
func (c *Client) Steal(ctx context.Context, namespace, name, holder string, ttl time.Duration, forceToken string) (*Lease, error) {
	body := map[string]any{"holder": holder, "ttl": ttl.String(), "reason": "client"}
	var response struct {
		Token     string    `json:"token"`
		Depth     int       `json:"depth"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	err := c.do(ctx, http.MethodPost, lockPath(namespace, name)+"/steal", body, forceToken, &response)
	if err != nil {
		return nil, err
	}
	return &Lease{Namespace: namespace, Name: name, Holder: holder, Token: response.Token, Depth: response.Depth, ExpiresAt: response.ExpiresAt, TTL: effectiveTTL(ttl, response.ExpiresAt)}, nil
}
func (c *Client) Delete(ctx context.Context, namespace, name, forceToken string) error {
	return c.do(ctx, http.MethodDelete, lockPath(namespace, name), nil, forceToken, nil)
}
func (c *Client) do(ctx context.Context, method, path string, body any, forceToken string, target any) error {
	var lastErr error
	attempts := 0
	// FIX: retry only read-only/listen requests; replaying acquire/release can mutate twice.
	if method == http.MethodGet || strings.HasSuffix(path, "/watch") {
		attempts = c.retries
	}
	for attempt := 0; attempt <= attempts; attempt++ {
		if attempt > 0 {
			delay := time.Duration(1<<(attempt-1)) * 50 * time.Millisecond
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
		err := c.doOnce(ctx, method, path, body, forceToken, target)
		if err == nil {
			return nil
		}
		var apiErr *Error
		if errors.As(err, &apiErr) {
			return err
		}
		lastErr = err
	}
	return lastErr
}
func (c *Client) doOnce(ctx context.Context, method, path string, body any, forceToken string, target any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if forceToken != "" {
		request.Header.Set("X-Force-Token", forceToken)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if method == http.MethodGet && (path == "/api/v1/locks" || strings.HasPrefix(path, "/api/v1/locks?")) {
		if response.StatusCode >= 400 {
			return fmt.Errorf("lockd: http %d", response.StatusCode)
		}
		if target != nil {
			return json.NewDecoder(response.Body).Decode(target)
		}
		return nil
	}
	var envelope responseEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return err
	}
	if envelope.Code != 0 {
		return &Error{Code: envelope.Code, Msg: envelope.Msg}
	}
	if target != nil && len(envelope.Data) > 0 {
		return json.Unmarshal(envelope.Data, target)
	}
	return nil
}
func lockPath(namespace, name string) string {
	return "/api/v1/locks/" + url.PathEscape(namespace) + "/" + url.PathEscape(name)
}
func lockKey(namespace, name string) string { return namespace + ":" + name }
func (c *Client) ActiveLeases() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.leases)
}

// removeLease drops a lease from the local cache. It only takes the lock for
// the map delete — never across a network call or user callback.
func (c *Client) removeLease(namespace, name string) {
	c.mu.Lock()
	delete(c.leases, lockKey(namespace, name))
	c.mu.Unlock()
}
func effectiveTTL(requested time.Duration, expiresAt time.Time) time.Duration {
	if requested > 0 {
		return requested
	}
	return min(10*time.Minute, max(time.Second, time.Until(expiresAt).Round(time.Second)))
}
