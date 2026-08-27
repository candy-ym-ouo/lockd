package client

import (
	"encoding/json"
	"testing"
	"time"
)

// TestLeaseJSONOmitsMutex confirms the added unexported sync.Mutex is not part of
// the serialized lease (lockctl marshals leases to stdout).
func TestLeaseJSONOmitsMutex(t *testing.T) {
	l := &Lease{Namespace: "n", Name: "l", Holder: "h", Token: "tk", Depth: 1, ExpiresAt: time.Now(), TTL: 30 * time.Second}
	b, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for k := range m {
		if k == "mu" {
			t.Fatalf("mutex leaked into JSON: %s", b)
		}
	}
}
