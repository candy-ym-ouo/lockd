package api

import (
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	core "lockd/internal/lock"
	"lockd/internal/logger"
	"lockd/internal/metrics"
)

func TestBug02_ConcurrentRequestTrackingIsSafe(t *testing.T) {
	service := core.NewService(core.NewRegistry(nil, 10, time.Second), core.NewBus(), &metrics.Metrics{}, "secret")
	handler := New(service, logger.New(io.Discard, "error"), nil).WithRequestTracking(true).Handler()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 80; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < 200; iteration++ {
				handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/healthz", nil))
			}
		}()
	}
	close(start)
	wg.Wait()
}
