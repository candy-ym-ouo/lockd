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
func requestLogging(log *logger.Logger, track bool, recent map[string]time.Time, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		if track {
			recent[r.URL.Path] = started
		}
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
				log.Error("http_panic", map[string]any{"error": recovered, "stack": string(debug.Stack())})
				writeError(w, &internalError{})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type internalError struct{}

func (*internalError) Error() string { return "internal error" }
