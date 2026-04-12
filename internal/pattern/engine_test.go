package pattern

import (
	"context"
	"testing"
	"time"

	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/ingest"
)

func defaultEngineConfig() PatternEngineConfig {
	return PatternEngineConfig{
		Drain:              DrainConfig{Depth: 4, SimilarityThreshold: 0.5, MaxChildren: 100, MaxPatterns: 1000},
		ExtractJSONMessage: true,
	}
}

func sendLines(in chan<- ingest.LogLine, lines []ingest.LogLine) {
	for _, l := range lines {
		in <- l
	}
}

func collectN(out <-chan ingest.LogLine, n int, d time.Duration) []ingest.LogLine {
	var result []ingest.LogLine
	deadline := time.After(d)
	for len(result) < n {
		select {
		case l, ok := <-out:
			if !ok {
				return result
			}
			result = append(result, l)
		case <-deadline:
			return result
		}
	}
	return result
}

// TestEngine_StampsPatternID verifies that each output line has PatternID and PatternTemplate set.
func TestEngine_StampsPatternID(t *testing.T) {
	pe := NewPatternEngine(defaultEngineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 5)
	out := pe.Run(ctx, in)

	in <- ingest.LogLine{Service: "svc", Level: "ERROR", Raw: "connection timeout to server"}
	lines := collectN(out, 1, 2*time.Second)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if lines[0].PatternID == "" {
		t.Error("expected non-empty PatternID")
	}
	if lines[0].PatternTemplate == "" {
		t.Error("expected non-empty PatternTemplate")
	}
}

// TestEngine_PreservesRaw verifies that Raw is not modified.
func TestEngine_PreservesRaw(t *testing.T) {
	pe := NewPatternEngine(defaultEngineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 5)
	out := pe.Run(ctx, in)

	raw := "{\"msg\":\"connection failed\",\"level\":\"error\"}"
	in <- ingest.LogLine{Service: "svc", Level: "ERROR", Raw: raw}
	lines := collectN(out, 1, 2*time.Second)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if lines[0].Raw != raw {
		t.Errorf("Raw was modified: got %q, want %q", lines[0].Raw, raw)
	}
}

// TestEngine_SameLinesProduceSamePattern verifies deterministic pattern assignment.
func TestEngine_SameLinesProduceSamePattern(t *testing.T) {
	pe := NewPatternEngine(defaultEngineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 10)
	out := pe.Run(ctx, in)

	for i := 0; i < 3; i++ {
		in <- ingest.LogLine{Service: "svc", Level: "ERROR", Raw: "disk write error on partition"}
	}
	lines := collectN(out, 3, 2*time.Second)
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	id := lines[0].PatternID
	for _, l := range lines[1:] {
		if l.PatternID != id {
			t.Errorf("inconsistent PatternID: %s vs %s", id, l.PatternID)
		}
	}
}

// TestEngine_ClosesOutputWhenInputCloses verifies pipeline close propagation.
func TestEngine_ClosesOutputWhenInputCloses(t *testing.T) {
	pe := NewPatternEngine(defaultEngineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 5)
	out := pe.Run(ctx, in)
	close(in)

	select {
	case _, ok := <-out:
		if ok {
			t.Error("expected out to be closed")
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for out to close")
	}
}

// TestEngine_ExtractJSONFalse disables JSON extraction, only regex normalization.
func TestEngine_ExtractJSONFalse(t *testing.T) {
	cfg := PatternEngineConfig{
		Drain:              DrainConfig{Depth: 4, SimilarityThreshold: 0.5, MaxChildren: 100, MaxPatterns: 1000},
		ExtractJSONMessage: false,
	}
	pe := NewPatternEngine(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 5)
	out := pe.Run(ctx, in)

	raw := "{\"msg\":\"connection failed\"}"
	in <- ingest.LogLine{Service: "svc", Level: "ERROR", Raw: raw}
	lines := collectN(out, 1, 2*time.Second)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	// PatternID should still be set (even without JSON extraction).
	if lines[0].PatternID == "" {
		t.Error("expected PatternID even without JSON extraction")
	}
	// Raw should still be preserved.
	if lines[0].Raw != raw {
		t.Errorf("Raw was modified: got %q", lines[0].Raw)
	}
}
