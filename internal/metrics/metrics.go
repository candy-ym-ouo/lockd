package metrics

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type Metrics struct {
	acquire       atomic.Uint64
	acquireLocked atomic.Uint64
	renew         atomic.Uint64
	renewFail     atomic.Uint64
	expiry        atomic.Uint64
	steal         atomic.Uint64
	notify        atomic.Uint64
	mu            sync.Mutex
	trackByNS     bool
	acquireByNS   map[string]uint64
}

func New(trackByNS bool) *Metrics {
	m := &Metrics{trackByNS: trackByNS}
	if !trackByNS {
		m.acquireByNS = make(map[string]uint64)
	}
	return m
}
func (m *Metrics) IncAcquire(namespace string) {
	m.acquire.Add(1)
	if m.trackByNS {
		m.mu.Lock()
		m.acquireByNS[namespace]++
		m.mu.Unlock()
	}
}
func (m *Metrics) IncAcquireLocked() { m.acquireLocked.Add(1) }
func (m *Metrics) IncRenew()         { m.renew.Add(1) }
func (m *Metrics) IncRenewFail()     { m.renewFail.Add(1) }
func (m *Metrics) IncExpiry()        { m.expiry.Add(1) }
func (m *Metrics) IncSteal()         { m.steal.Add(1) }
func (m *Metrics) IncNotify()        { m.notify.Add(1) }

func (m *Metrics) WritePrometheus(w io.Writer, locks, waiters int, byNamespace map[string]int) {
	fmt.Fprintln(w, "# TYPE lockd_locks_total gauge")
	for namespace, count := range byNamespace {
		fmt.Fprintf(w, "lockd_locks_total{namespace=%q} %d\n", namespace, count)
	}
	fmt.Fprintln(w, "# TYPE lockd_waiters_total gauge")
	fmt.Fprintf(w, "lockd_waiters_total %d\n", waiters)
	fmt.Fprintln(w, "# TYPE lockd_acquire_total counter")
	fmt.Fprintf(w, "lockd_acquire_total %d\n", m.acquire.Load())
	fmt.Fprintln(w, "# TYPE lockd_acquire_locked_total counter")
	fmt.Fprintf(w, "lockd_acquire_locked_total %d\n", m.acquireLocked.Load())
	fmt.Fprintln(w, "# TYPE lockd_renew_total counter")
	fmt.Fprintf(w, "lockd_renew_total %d\n", m.renew.Load())
	fmt.Fprintln(w, "# TYPE lockd_renew_fail_total counter")
	fmt.Fprintf(w, "lockd_renew_fail_total %d\n", m.renewFail.Load())
	fmt.Fprintln(w, "# TYPE lockd_expiry_total counter")
	fmt.Fprintf(w, "lockd_expiry_total %d\n", m.expiry.Load())
	fmt.Fprintln(w, "# TYPE lockd_steal_total counter")
	fmt.Fprintf(w, "lockd_steal_total %d\n", m.steal.Load())
	fmt.Fprintln(w, "# TYPE lockd_notify_total counter")
	fmt.Fprintf(w, "lockd_notify_total %d\n", m.notify.Load())
	fmt.Fprintf(w, "# lockd_registry_locks %d\n", locks)
}
