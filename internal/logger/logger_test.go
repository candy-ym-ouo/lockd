package logger

import (
	"bytes"
	"strings"
	"testing"
)

// TestDisabledWriterIsSafeNoOp guards the original bug: DisabledWriter used
// to return a nil *disabledWriter wrapped in a non-nil io.Writer interface.
// logger.New's nil guard did not catch it, so the first Write call (e.g.
// from request logging on /api/v1/healthz) panicked with a nil-pointer
// dereference, dropping the connection. The sink must now be a real no-op.
func TestDisabledWriterIsSafeNoOp(t *testing.T) {
	w := DisabledWriter()
	if w == nil {
		t.Fatal("DisabledWriter returned a nil writer")
	}
	// It must not be an interface holding a nil pointer, otherwise the next
	// Write dereferences nil and panics.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("writing to DisabledWriter panicked: %v", r)
		}
	}()
	n, err := w.Write([]byte("anything"))
	if err != nil {
		t.Fatalf("DisabledWriter returned error: %v", err)
	}
	if n != len("anything") {
		t.Fatalf("DisabledWriter reported %d bytes written, want %d", n, len("anything"))
	}
}

// TestLoggerWithDisabledWriter covers the full wiring used by the server when
// -disable-logging is set: a Logger backed by DisabledWriter must accept
// every log method without panicking and produce no observable output.
func TestLoggerWithDisabledWriter(t *testing.T) {
	log := New(DisabledWriter(), "debug")
	for _, fn := range []func(string, map[string]any){
		log.Debug, log.Info, log.Warn, log.Error,
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("log method panicked: %v", r)
				}
			}()
			fn("event", map[string]any{"k": "v"})
		}()
	}
}

// TestLoggerNilWriterSafe ensures the log path — which the HTTP panic
// recovery handler also relies on — never panics on a nil receiver or writer.
func TestLoggerNilWriterSafe(t *testing.T) {
	var nilLog *Logger
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nil-receiver log panicked: %v", r)
		}
	}()
	nilLog.Info("noop", nil)

	// A Logger with a nil out (constructed by skipping New) must also be safe.
	bad := &Logger{}
	bad.Error("noop", map[string]any{"x": 1})
}

func TestLoggerWritesAndRespectsLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	log := New(buf, "warn")
	log.Debug("skipped", nil)
	log.Warn("written", map[string]any{"k": "v"})
	out := buf.String()
	if strings.Contains(out, "skipped") {
		t.Fatalf("debug record leaked past warn level: %s", out)
	}
	if !strings.Contains(out, "written") || !strings.Contains(out, "\"level\":\"warn\"") {
		t.Fatalf("warn record not written as expected: %s", out)
	}
}
