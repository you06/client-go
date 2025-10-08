package util

import (
	"context"
	"testing"
)

func TestWithTracerAndGetTracer(t *testing.T) {
	ctx := context.Background()

	want := "test-tag"
	ctx = WithTracer(ctx, want)

	got, ok := GetTracer(ctx)
	if !ok {
		t.Fatalf("expected tracer to be present")
	}
	if got != want {
		t.Fatalf("unexpected tracer value: got %q want %q", got, want)
	}
}

func TestGetTracerMissing(t *testing.T) {
	ctx := context.Background()

	got, ok := GetTracer(ctx)
	if ok {
		t.Fatalf("expected tracer to be absent")
	}
	if got != "" {
		t.Fatalf("expected empty tracer value when absent, got %q", got)
	}
}

func TestGetTracerWrongType(t *testing.T) {
	ctx := context.WithValue(context.Background(), TracerKey, 42)

	got, ok := GetTracer(ctx)
	if ok {
		t.Fatalf("expected type assertion to fail")
	}
	if got != "" {
		t.Fatalf("expected empty tracer value when type assertion fails, got %q", got)
	}
}
