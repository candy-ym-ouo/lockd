package lock

import (
	"testing"
	"time"

	"lockd/internal/metrics"
)

func TestBug05_ListSnapshotsRemainCallerOwned(t *testing.T) {
	service := NewService(NewRegistry(nil, 10, time.Second), NewBus(), &metrics.Metrics{}, "secret")
	for _, item := range []CreateOptions{
		{Namespace: "a", Name: "one", DefaultTTL: time.Second},
		{Namespace: "a", Name: "two", DefaultTTL: time.Second},
		{Namespace: "b", Name: "three", DefaultTTL: time.Second},
	} {
		if _, err := service.Create(item); err != nil {
			t.Fatal(err)
		}
	}
	first := service.List("", "all")
	if len(first) != 3 {
		t.Fatalf("first list length = %d, want 3", len(first))
	}
	want := []string{first[0].FullName, first[1].FullName, first[2].FullName}
	second := service.List("b", "all")
	if len(second) != 1 || second[0].FullName != "b:three" {
		t.Fatalf("unexpected filtered list: %+v", second)
	}
	for index := range want {
		if first[index].FullName != want[index] {
			t.Fatalf("first snapshot changed at %d: got %q, want %q", index, first[index].FullName, want[index])
		}
	}
}
