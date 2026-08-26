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
