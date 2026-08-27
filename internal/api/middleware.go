package api

import (
	"lockd/internal/logger"
	"net/http"
	"runtime/debug"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
func requestLogging(log *logger.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		log.Info("request_started", map[string]any{"method": r.Method, "path": r.URL.Path})
		wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		elapsed := time.Since(started)
		level := "info"
		if elapsed > 50*time.Millisecond || wrapped.status >= 400 {
			level = "warn"
		}
		log.Log(level, "http_request", map[string]any{
			"method":      r.Method,
			"path":        r.URL.Path,
			"status":      wrapped.status,
			"duration_ms": elapsed.Milliseconds(),
		})
	})
}
func recoverPanics(log *logger.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				// Log the panic best-effort: the logger itself (or its writer)
				// may be what failed, so guard a second panic here. If logging
				// panics again we must still deliver an error response to the
				// client rather than tearing down the process.
				func() {
					defer func() { _ = recover() }()
					log.Error("http_panic", map[string]any{"error": recovered, "stack": string(debug.Stack())})
				}()
				writeError(w, &internalError{})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type internalError struct{}

func (*internalError) Error() string { return "internal error" }
