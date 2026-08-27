package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestHTTPConcurrentListPaginateAndDetail reproduces the reported scenario:
// after creating a few dozen locks (each with an active wait queue), many
// goroutines hammer GET /locks, GET /locks?offset&limit, and the single-lock
// detail endpoint concurrently. Run with -race to confirm the responses no
// longer alias shared registry buffers while being encoded.
func TestHTTPConcurrentListPaginateAndDetail(t *testing.T) {
	server := newTestServer(t)
	const numLocks = 40

	for i := 0; i < numLocks; i++ {
		name := fmt.Sprintf("l-%d", i)
		postJSON(t, server.URL+"/api/v1/locks", map[string]any{
			"namespace": "ns", "name": name, "reentrant": false, "ttl": "30s",
		}, http.StatusOK, 0)
		// Hold the lock so the following wait:true acquires queue behind it.
		postJSON(t, server.URL+"/api/v1/locks/ns/"+name+"/acquire",
			map[string]any{"holder": "owner"}, http.StatusOK, 0)
		// Two blocked waiters form a stable queue of length 2; they are expected
		// to time out, so fire them without asserting on the outcome.
		for w := 0; w < 2; w++ {
			holder := fmt.Sprintf("w-%d-%d", i, w)
			go func() {
				body, _ := json.Marshal(map[string]any{
					"holder": holder, "wait": true, "wait_timeout": "3s",
				})
				resp, err := server.Client().Post(
					server.URL+"/api/v1/locks/ns/"+name+"/acquire",
					"application/json", bytes.NewReader(body))
				if err == nil {
					resp.Body.Close()
				}
			}()
		}
	}
	// Let waiters enqueue before the readers start hammering the endpoints.
	time.Sleep(120 * time.Millisecond)

	const readers = 12
	const iterations = 30
	var wg sync.WaitGroup
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			client := server.Client()
			for it := 0; it < iterations; it++ {
				// Full list.
				if resp, err := client.Get(server.URL + "/api/v1/locks?namespace=ns"); err == nil {
					var locks []map[string]any
					_ = json.NewDecoder(resp.Body).Decode(&locks)
					resp.Body.Close()
				}
				// Paginated list.
				offset := (it + seed) % (numLocks + 1)
				if resp, err := client.Get(server.URL + fmt.Sprintf("/api/v1/locks?namespace=ns&offset=%d&limit=7", offset)); err == nil {
					var page []map[string]any
					_ = json.NewDecoder(resp.Body).Decode(&page)
					resp.Body.Close()
				}
				// Single-lock detail.
				name := fmt.Sprintf("l-%d", (it+seed)%numLocks)
				if resp, err := client.Get(server.URL + "/api/v1/locks/ns/" + name); err == nil {
					body := json.NewDecoder(resp.Body)
					var envelope struct {
						Data map[string]any `json:"data"`
					}
					if err := body.Decode(&envelope); err != nil {
						t.Errorf("decode detail %s: %v", name, err)
					}
					if queue, ok := envelope.Data["queue"].([]any); ok {
						for _, entry := range queue {
							if m, ok := entry.(map[string]any); ok {
								if holder, _ := m["holder"].(string); holder == "" {
									t.Errorf("empty holder in %s queue: %v", name, queue)
									break
								}
							}
						}
					}
					resp.Body.Close()
				}
			}
		}(r)
	}
	wg.Wait()
}
