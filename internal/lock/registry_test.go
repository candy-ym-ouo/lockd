package lock

import (
	"errors"
	"testing"
	"time"
)

func TestRegistryPolicyAndQuota(t *testing.T) {
	registry := NewRegistry([]string{"allowed"}, 1, 5*time.Second)
	if _, err := registry.Create(CreateOptions{Namespace: "denied", Name: "one", Reentrant: true}); !errors.Is(err, ErrNamespaceDenied) {
		t.Fatalf("expected namespace denial, got %v", err)
	}
	if _, err := registry.Create(CreateOptions{Namespace: "allowed", Name: "one", Reentrant: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Create(CreateOptions{Namespace: "allowed", Name: "one", Reentrant: true}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	if _, err := registry.Create(CreateOptions{Namespace: "allowed", Name: "two", Reentrant: true}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected quota error, got %v", err)
	}
}

func TestRegistryValidationAndList(t *testing.T) {
	registry := NewRegistry(nil, 10, 5*time.Second)
	if _, err := registry.Create(CreateOptions{Namespace: "bad/name", Name: "one"}); !errors.Is(err, ErrInvalid) {
		var lockErr *Error
		if !errors.As(err, &lockErr) || lockErr.Code != CodeInvalid {
			t.Fatalf("expected validation error, got %v", err)
		}
	}
	_, _ = registry.Create(CreateOptions{Namespace: "b", Name: "two", Reentrant: true})
	_, _ = registry.Create(CreateOptions{Namespace: "a", Name: "one", Reentrant: true})
	views := registry.List("", "idle")
	if len(views) != 2 || views[0].FullName != "a:one" || views[1].FullName != "b:two" {
		t.Fatalf("unexpected sorted list: %#v", views)
	}
}
