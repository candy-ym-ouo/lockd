package client

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type bug07Transport struct {
	renews atomic.Int32
}

func (transport *bug07Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	var payload []byte
	if strings.HasSuffix(request.URL.Path, "/renew") {
		transport.renews.Add(1)
		payload = []byte(`{"code":0,"msg":"ok","data":{"expires_at":"2030-01-01T00:00:00Z"}}`)
	} else {
		payload = []byte(`{"code":0,"msg":"ok","data":{"token":"tk","holder":"h","depth":1,"expires_at":"2030-01-01T00:00:00Z"}}`)
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
}

func TestBug07_RenewerUsesItsOwnParentContext(t *testing.T) {
	transport := &bug07Transport{}
	client := New("http://lockd.invalid")
	client.SetHTTPClient(&http.Client{Transport: transport})
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	lease, err := client.Acquire(requestCtx, "n", "l", "h", AcquireOptions{TTL: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cancelRequest()
	renewerCtx, cancelRenewer := context.WithCancel(context.Background())
	stop := client.StartRenewer(renewerCtx, lease, nil)
	defer func() {
		stop()
		cancelRenewer()
	}()
	deadline := time.Now().Add(600 * time.Millisecond)
	for transport.renews.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if transport.renews.Load() == 0 {
		t.Fatal("renewer stopped with the completed acquire request context")
	}
}
