package metrics

import "testing"

func TestBug03_ByNamespaceAcquireCounterIsInitialized(t *testing.T) {
	metricSet := New(true)
	metricSet.IncAcquire("orders")
	metricSet.IncAcquire("orders")
	metricSet.mu.Lock()
	got := metricSet.acquireByNS["orders"]
	metricSet.mu.Unlock()
	if got != 2 {
		t.Fatalf("namespace acquire count = %d, want 2", got)
	}
}
