package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSSEConnectDisconnectWhileLeaseChurn reproduces the reported crash: many
// SSE clients repeatedly connect to /api/v1/events and immediately disconnect,
// while the server keeps acquiring, releasing and expiring leases. Under the
// old Bus this raced cancel()/close against Broadcast's send, panicking with
// "send on closed channel". The fixed protocol must survive this churn while
// still delivering events and counting notifications.
func TestSSEConnectDisconnectWhileLeaseChurn(t *testing.T) {
	server := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Lock that churns through acquire / release / expiry. TTL must be >= 1s
	// (the server minimum), so we drive churn by acquire+release in a tight
	// loop and let the test run long enough for the background expirer to
	// also fire an expiry event.
	postJSON(t, server.URL+"/api/v1/locks", map[string]any{
		"namespace": "churn", "name": "res", "reentrant": true, "ttl": "1s",
	}, http.StatusOK, 0)

	var wg sync.WaitGroup
	const flashers = 40
	wg.Add(flashers)
	for i := 0; i < flashers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				if ctx.Err() != nil {
					return
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/events", nil)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					continue
				}
				// Connect-then-immediately-disconnect: closing the body makes
				// the server's handleEvents return and run its cancel(), which
				// removes the subscriber and closes its channel.
				resp.Body.Close()
			}
		}()
	}

	// Lease churn interleaved with the SSE churn, then verify the notify
	// counter still advances (delivery + notification metrics still work).
	base := server.URL
	delivered := make(chan int, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		notifyBefore := notifyTotal(base)
		deadline := time.Now().Add(1300 * time.Millisecond)
		for time.Now().Before(deadline) {
			if ctx.Err() != nil {
				break
			}
			token := acquire(t, base)
			release(t, base, token)
		}
		// Let the bus drain, then read the notify counter.
		time.Sleep(50 * time.Millisecond)
		delivered <- notifyTotal(base) - notifyBefore
	}()

	wg.Wait()
	close(delivered)
	if d := <-delivered; d <= 0 {
		t.Fatalf("expected notify metric to advance during churn, got %d", d)
	}
}

func acquire(t *testing.T, server string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"holder": "h"})
	resp, err := http.Post(server+"/api/v1/locks/churn/res/acquire", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env struct {
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	payload, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(payload, &env); err != nil || env.Data.Token == "" {
		t.Fatalf("acquire failed: %s", payload)
	}
	return env.Data.Token
}

func release(t *testing.T, server, token string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"token": token})
	resp, err := http.Post(server+"/api/v1/locks/churn/res/release", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func notifyTotal(server string) int {
	resp, err := http.Get(server + "/api/v1/metrics")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "lockd_notify_total ") {
			n, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "lockd_notify_total ")))
			return n
		}
	}
	return 0
}
