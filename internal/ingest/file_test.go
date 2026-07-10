package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestFileSource_ServiceOverride verifies that when FileConfig.Service is set,
// it overrides the per-line "service" field (and applies even when the line
// omits "service" entirely) — the per-service pipeline model.
func TestFileSource_ServiceOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "svc.ndjson")
	content := `{"timestamp":"2026-04-12T10:00:01Z","raw":"ERROR boom"}
{"service":"wrong-name","timestamp":"2026-04-12T10:00:02Z","raw":"ERROR kaboom"}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	src := NewFileSource(FileConfig{Path: path, Service: "payment-svc"})
	out, err := src.Stream(context.Background())
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	var got []LogLine
	for line := range out {
		got = append(got, line)
	}

	if len(got) != 2 {
		t.Fatalf("got %d lines, want 2", len(got))
	}
	for i, line := range got {
		if line.Service != "payment-svc" {
			t.Errorf("line %d: service = %q, want %q (override)", i, line.Service, "payment-svc")
		}
	}
}

// TestFileSource_NoOverrideUsesLineService verifies that without an override,
// the per-line "service" field is preserved (mixed-file behavior).
func TestFileSource_NoOverrideUsesLineService(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "svc.ndjson")
	content := `{"service":"auth-service","timestamp":"2026-04-12T10:00:01Z","raw":"ERROR boom"}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	src := NewFileSource(FileConfig{Path: path})
	out, err := src.Stream(context.Background())
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	var got []LogLine
	for line := range out {
		got = append(got, line)
	}
	if len(got) != 1 {
		t.Fatalf("got %d lines, want 1", len(got))
	}
	if got[0].Service != "auth-service" {
		t.Errorf("service = %q, want %q", got[0].Service, "auth-service")
	}
}
