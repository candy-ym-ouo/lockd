package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	core "lockd/internal/lock"
	"lockd/internal/logger"
	"lockd/internal/metrics"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	service := core.NewService(core.NewRegistry(nil, 100, 2*time.Second), core.NewBus(), &metrics.Metrics{}, "secret")
	handler := New(service, logger.New(io.Discard, "error"), nil).Handler()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

// newDisabledLoggingServer wires the logger exactly as the daemon does under
// -disable-logging: the output sink is logger.DisabledWriter(). Previously
// this returned a nil *disabledWriter hidden inside a non-nil io.Writer, so
// the first request (e.g. a health check) panicked inside request logging
// and the recovery handler's own log.Error panicked again, dropping the
// connection and crashing the request goroutine.
func newDisabledLoggingServer(t *testing.T) *httptest.Server {
	t.Helper()
	service := core.NewService(core.NewRegistry(nil, 100, 2*time.Second), core.NewBus(), &metrics.Metrics{}, "secret")
	handler := New(service, logger.New(logger.DisabledWriter(), "info"), nil).Handler()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

// TestHealthzWithDisabledLogging reproduces the reported failure: with logging
// disabled, the first healthz request must succeed and the connection must not
// be dropped by a nil-pointer panic in the request logger.
func TestHealthzWithDisabledLogging(t *testing.T) {
	server := newDisabledLoggingServer(t)
	for i := 0; i < 3; i++ {
		resp, err := http.Get(server.URL + "/api/v1/healthz")
		if err != nil {
			t.Fatalf("request %d failed: %v (connection dropped — logger panicked)", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status %d, body %s", i, resp.StatusCode, body)
		}
	}
}

// TestRecoverPanicsDoesNotPanicAgainOnLog verifies the panic recovery path is
// robust when logging itself would fail. The recovery handler calls
// log.Error to record the panic; if that call panicked again (as it did when
// the disabled writer was a nil pointer), the request goroutine would abort
// without writing a response. We force a panic through a handler and assert a
// clean 500 is returned.
func TestRecoverPanicsDoesNotPanicAgainOnLog(t *testing.T) {
	// A logger whose writer panics on Write simulates the historical failure
	// where the disabled sink was a nil pointer, and also protects against any
	// future writer that misbehaves.
	panicWriter := &panickingWriter{}
	log := logger.New(panicWriter, "info")
	mux := http.NewServeMux()
	mux.HandleFunc("/boom", func(http.ResponseWriter, *http.Request) {
		panic("kaboom")
	})
	handler := recoverPanics(log, mux)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/boom")
	if err != nil {
		t.Fatalf("request failed: %v (recovery path panicked a second time)", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 after recovered panic, got %d", resp.StatusCode)
	}
}

type panickingWriter struct{}

func (*panickingWriter) Write([]byte) (int, error) { panic("writer exploded") }

func TestHTTPWorkflow(t *testing.T) {
	server := newTestServer(t)
	postJSON(t, server.URL+"/api/v1/locks", map[string]any{
		"namespace": "orders", "name": "pay", "reentrant": true, "ttl": "2s",
	}, http.StatusOK, 0)
	response := postJSON(t, server.URL+"/api/v1/locks/orders/pay/acquire", map[string]any{
		"holder": "svc-1", "wait": false,
	}, http.StatusOK, 0)
	var envelope struct {
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil || envelope.Data.Token == "" {
		t.Fatalf("missing token: %s, %v", response, err)
	}
	postJSON(t, server.URL+"/api/v1/locks/orders/pay/acquire", map[string]any{
		"holder": "svc-2", "wait": false,
	}, http.StatusConflict, int(core.CodeLocked))
	postJSON(t, server.URL+"/api/v1/locks/orders/pay/release", map[string]any{
		"token": envelope.Data.Token,
	}, http.StatusOK, 0)

	listResponse, err := http.Get(server.URL + "/api/v1/locks?namespace=orders")
	if err != nil {
		t.Fatal(err)
	}
	defer listResponse.Body.Close()
	var locks []core.View
	if err := json.NewDecoder(listResponse.Body).Decode(&locks); err != nil || len(locks) != 1 || locks[0].State != "idle" {
		t.Fatalf("unexpected lock list: %#v, %v", locks, err)
	}

	healthResponse, err := http.Get(server.URL + "/api/v1/healthz")
	if err != nil || healthResponse.StatusCode != http.StatusOK {
		t.Fatalf("health check failed: %v", err)
	}
	healthResponse.Body.Close()
}

func TestHTTPWaitTimeoutAndForceToken(t *testing.T) {
	server := newTestServer(t)
	postJSON(t, server.URL+"/api/v1/locks", map[string]any{"namespace": "n", "name": "l"}, http.StatusOK, 0)
	first := postJSON(t, server.URL+"/api/v1/locks/n/l/acquire", map[string]any{"holder": "one"}, http.StatusOK, 0)
	postJSON(t, server.URL+"/api/v1/locks/n/l/acquire", map[string]any{
		"holder": "two", "wait": true, "wait_timeout": "15ms",
	}, http.StatusRequestTimeout, int(core.CodeWaitTimeout))

	requestBody, _ := json.Marshal(map[string]any{"holder": "ops", "ttl": "2s"})
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/api/v1/locks/n/l/steal", bytes.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Force-Token", "secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("steal failed: %v, status %v, first %s", err, response.Status, first)
	}
	response.Body.Close()
}

func TestHTTPRejectsTrailingJSONValue(t *testing.T) {
	server := newTestServer(t)
	body := []byte(`{"namespace":"n","name":"one"} {"name":"two"}`)
	response, err := http.Post(server.URL+"/api/v1/locks", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("trailing JSON was accepted: status=%d body=%s", response.StatusCode, payload)
	}
}

func postJSON(t *testing.T, target string, body any, status, code int) []byte {
	t.Helper()
	data, _ := json.Marshal(body)
	response, err := http.Post(target, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	if response.StatusCode != status {
		t.Fatalf("unexpected HTTP status %d, want %d: %s", response.StatusCode, status, payload)
	}
	if code >= 0 {
		var parsed struct {
			Code int `json:"code"`
		}
		if err := json.Unmarshal(payload, &parsed); err != nil || parsed.Code != code {
			t.Fatalf("unexpected code in %s: %v", payload, err)
		}
	}
	return payload
}
