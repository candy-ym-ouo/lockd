package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
)

type bug08Transport struct{}

func (bug08Transport) RoundTrip(*http.Request) (*http.Response, error) {
	payload := []byte(`{"code":0,"msg":"ok","data":{"token":"tk","holder":"h","depth":1,"expires_at":"2030-01-01T00:00:00Z"}}`)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
}

func TestBug08_SharedClientLeaseCacheIsSynchronized(t *testing.T) {
	client := New("http://lockd.invalid")
	client.SetHTTPClient(&http.Client{Transport: bug08Transport{}})
	for round := 0; round < 10; round++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for worker := 0; worker < 128; worker++ {
			worker := worker
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_, _ = client.Acquire(context.Background(), "ns", fmt.Sprintf("lock-%d-%d", round, worker), "h", AcquireOptions{})
			}()
			go func() {
				defer wg.Done()
				<-start
				for iteration := 0; iteration < 100; iteration++ {
					_ = client.ActiveLeases()
				}
			}()
		}
		close(start)
		wg.Wait()
	}
}
