package api

import (
	"io/fs"
	core "lockd/internal/lock"
	"lockd/internal/logger"
	"net/http"
	"time"
)

type API struct {
	service *core.Service
	log     *logger.Logger
	web     fs.FS
}

func New(service *core.Service, log *logger.Logger, webFS fs.FS) *API {
	return &API{service: service, log: log, web: webFS}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/locks", a.handleLocks)
	mux.HandleFunc("/api/v1/locks/", a.handleLockPath)
	mux.HandleFunc("/api/v1/events", a.handleEvents)
	mux.HandleFunc("/api/v1/metrics", a.handleMetrics)
	mux.HandleFunc("/api/v1/healthz", a.handleHealth)
	if a.web != nil {
		mux.Handle("/", http.FileServer(http.FS(a.web)))
	}
	return recoverPanics(a.log, requestLogging(a.log, mux))
}
func (a *API) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, &core.Error{Code: core.CodeInternal, Message: "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	channel, cancel := a.service.Bus().Subscribe(64)
	defer cancel()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	namespace := r.URL.Query().Get("namespace")
	for {
		select {
		case event := <-channel:
			if namespace != "" && event.Namespace != namespace {
				continue
			}
			if encodeSSE(w, "lock", event) != nil {
				return
			}
			flusher.Flush()
		case now := <-heartbeat.C:
			if encodeSSE(w, "heartbeat", map[string]any{"at": now.UTC()}) != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
func (a *API) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	locks, waiters, namespaces := a.service.Registry().Stats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	a.service.Metrics().WritePrometheus(w, locks, waiters, namespaces)
}
func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	locks, waiters, _ := a.service.Registry().Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"uptime_sec": int64(time.Since(a.service.StartedAt()).Seconds()),
		"locks":      locks,
		"waiters":    waiters,
	})
}
