package notify

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestLogNotifier_Send(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ln := NewLogNotifier(logger)

	alert := Alert{
		Service:     "myapp",
		Level:       "ERROR",
		Count:       3,
		Window:      1 * time.Minute,
		SampleLines: []string{"line1", "line2"},
		Timestamp:   time.Now(),
	}

	err := ln.Send(context.Background(), alert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	if !stringContains(output, "ALERT") {
		t.Errorf("expected ALERT in output, got: %s", output)
	}
	if !stringContains(output, "myapp") {
		t.Errorf("expected service name in output, got: %s", output)
	}
	if !stringContains(output, "ERROR") {
		t.Errorf("expected level in output, got: %s", output)
	}
}

func TestLogNotifier_Name(t *testing.T) {
	ln := NewLogNotifier(nil)
	if ln.Name() != "log" {
		t.Errorf("name = %s, want log", ln.Name())
	}
}

func TestLogNotifier_NilLogger(t *testing.T) {
	ln := NewLogNotifier(nil)
	if ln.Logger == nil {
		t.Fatal("expected non-nil default logger")
	}
	// Should not panic
	err := ln.Send(context.Background(), Alert{Service: "svc", Level: "WARN", Count: 1, Window: time.Minute})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLogNotifier_PatternRendering(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ln := NewLogNotifier(logger)

	alert := Alert{
		Service: "myapp",
		Level:   "ERROR",
		Count:   5,
		Window:  1 * time.Minute,
		Patterns: []PatternSummary{
			{Template: "connection timeout to <*>", Count: 3, Level: "ERROR"},
			{Template: "disk write error", Count: 2, Level: "WARN"},
		},
		Timestamp: time.Now(),
	}

	err := ln.Send(context.Background(), alert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	if !stringContains(output, "connection timeout to") {
		t.Errorf("expected pattern template in output, got: %s", output)
	}
	if !stringContains(output, "3x") {
		t.Errorf("expected pattern count in output, got: %s", output)
	}
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// --- Anomaly rendering tests ---

func TestLogNotifier_PatternWithSpikeAnomaly(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ln := NewLogNotifier(logger)

	alert := Alert{
		Service: "svc",
		Level:   "ERROR",
		Count:   200,
		Window:  1 * time.Minute,
		Patterns: []PatternSummary{
			{Template: "timeout <*>", Count: 200, Level: "ERROR", Anomaly: AnomalySpike, ZScore: 4.2},
		},
	}
	if err := ln.Send(context.Background(), alert); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()
	if !stringContains(output, "SPIKE") {
		t.Errorf("expected SPIKE in output, got: %s", output)
	}
	if !stringContains(output, "4.2") {
		t.Errorf("expected z=4.2 in output, got: %s", output)
	}
}

func TestLogNotifier_PatternWithNewPattern(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ln := NewLogNotifier(logger)

	alert := Alert{
		Service: "svc",
		Level:   "ERROR",
		Count:   1,
		Window:  1 * time.Minute,
		Patterns: []PatternSummary{
			{Template: "new error <*>", Count: 1, Level: "ERROR", Anomaly: AnomalyNewPattern},
		},
	}
	if err := ln.Send(context.Background(), alert); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()
	if !stringContains(output, "NEW") {
		t.Errorf("expected NEW in output, got: %s", output)
	}
}

func TestLogNotifier_PatternWithNoAnomaly(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ln := NewLogNotifier(logger)

	alert := Alert{
		Service: "svc",
		Level:   "ERROR",
		Count:   5,
		Window:  1 * time.Minute,
		Patterns: []PatternSummary{
			{Template: "steady error", Count: 5, Level: "ERROR", Anomaly: AnomalyNone},
		},
	}
	if err := ln.Send(context.Background(), alert); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()
	if stringContains(output, "SPIKE") || stringContains(output, "NEW") || stringContains(output, "RATE-JUMP") {
		t.Errorf("expected no anomaly tag in output, got: %s", output)
	}
}
