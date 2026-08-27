package logger

import "testing"

func TestBug09_DisabledWriterAcceptsWrites(t *testing.T) {
	writer := DisabledWriter()
	if writer == nil {
		t.Fatal("disabled writer is nil")
	}
	payload := []byte("request")
	written, err := writer.Write(payload)
	if err != nil {
		t.Fatal(err)
	}
	if written != len(payload) {
		t.Fatalf("written = %d, want %d", written, len(payload))
	}
}
