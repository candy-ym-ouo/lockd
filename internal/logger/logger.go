package logger

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

type Logger struct {
	mu    sync.Mutex
	out   io.Writer
	level int
}

// DisabledWriter returns a sink that discards all log output safely. It is
// used when the service is started with -disable-logging so request handling
// can still flow through code paths that call the logger. The returned writer
// is a non-nil io.Writer, so logger.New's nil guard does not replace it, and
// every Write call is a no-op rather than a nil-pointer dereference.
func DisabledWriter() io.Writer {
	return io.Discard
}

var levels = map[string]int{"debug": 0, "info": 1, "warn": 2, "error": 3}

func New(out io.Writer, level string) *Logger {
	if out == nil {
		out = os.Stdout
	}
	parsed, ok := levels[strings.ToLower(level)]
	if !ok {
		parsed = levels["info"]
	}
	return &Logger{out: out, level: parsed}
}

func (l *Logger) Log(level, message string, fields map[string]any) {
	// A nil receiver or nil writer must never panic; logging is best-effort
	// and the log path is reused by the HTTP panic recovery handler, where a
	// second panic would abort the process and drop the client connection.
	// Guard before touching any receiver field (e.g. l.level).
	if l == nil || l.out == nil {
		return
	}
	value, ok := levels[strings.ToLower(level)]
	if !ok || value < l.level {
		return
	}
	record := make(map[string]any, len(fields)+3)
	record["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	record["level"] = strings.ToLower(level)
	record["msg"] = message
	for key, item := range fields {
		record[key] = item
	}
	data, err := json.Marshal(record)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(append(data, '\n'))
}
func (l *Logger) Debug(message string, fields map[string]any) { l.Log("debug", message, fields) }
func (l *Logger) Info(message string, fields map[string]any)  { l.Log("info", message, fields) }
func (l *Logger) Warn(message string, fields map[string]any)  { l.Log("warn", message, fields) }
func (l *Logger) Error(message string, fields map[string]any) { l.Log("error", message, fields) }
