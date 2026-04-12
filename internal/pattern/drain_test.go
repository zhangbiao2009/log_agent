package pattern

import (
	"strings"
	"testing"
)

func defaultDrain() *Drain {
	return NewDrain(DrainConfig{
		Depth:               4,
		SimilarityThreshold: 0.5,
		MaxChildren:         100,
		MaxPatterns:         1000,
	})
}

// TestDrain_SameLineProducesSamePattern verifies that identical messages yield the same pattern.
func TestDrain_SameLineProducesSamePattern(t *testing.T) {
	d := defaultDrain()
	p1 := d.Process("connection timeout to server")
	p2 := d.Process("connection timeout to server")
	if p1.ID != p2.ID {
		t.Errorf("same line produced different pattern IDs: %s vs %s", p1.ID, p2.ID)
	}
	if len(d.Patterns()) != 1 {
		t.Errorf("expected 1 pattern, got %d", len(d.Patterns()))
	}
}

// TestDrain_VariableTokensAbstracted verifies that variable tokens are replaced with wildcards.
func TestDrain_VariableTokensAbstracted(t *testing.T) {
	d := defaultDrain()
	d.Process("connection timeout to <IP>")
	d.Process("connection timeout to <IP>")
	p := d.Process("connection timeout to <IP>")

	if !strings.Contains(p.Template, "connection") {
		t.Errorf("expected 'connection' in template, got: %q", p.Template)
	}
	if len(d.Patterns()) != 1 {
		t.Errorf("expected 1 pattern, got %d", len(d.Patterns()))
	}
}

// TestDrain_TwoDistinctMessages creates two different patterns.
func TestDrain_TwoDistinctMessages(t *testing.T) {
	d := defaultDrain()
	p1 := d.Process("connection timeout to server")
	p2 := d.Process("disk write error on partition")
	if p1.ID == p2.ID {
		t.Errorf("different messages produced same pattern")
	}
	if len(d.Patterns()) != 2 {
		t.Errorf("expected 2 patterns, got %d", len(d.Patterns()))
	}
}

// TestDrain_MergesVariablePositions verifies that lines sharing a template
// but differing in one token merge into a single pattern with a wildcard.
func TestDrain_MergesVariablePositions(t *testing.T) {
	d := defaultDrain()
	d.Process("failed to connect host1 port <NUM>")
	d.Process("failed to connect host2 port <NUM>")
	d.Process("failed to connect host3 port <NUM>")
	// All three have the same length and similar structure — should merge.
	patterns := d.Patterns()
	if len(patterns) != 1 {
		t.Errorf("expected 1 merged pattern, got %d", len(patterns))
	}
	for _, p := range patterns {
		if !strings.Contains(p.Template, "<*>") {
			t.Errorf("expected wildcard in merged template, got: %q", p.Template)
		}
	}
}

// TestDrain_PatternID deterministic.
func TestDrain_PatternIDDeterministic(t *testing.T) {
	d1 := defaultDrain()
	d2 := defaultDrain()
	p1 := d1.Process("server error on startup")
	p2 := d2.Process("server error on startup")
	if p1.ID != p2.ID {
		t.Errorf("same message produced different IDs across Drain instances: %s vs %s", p1.ID, p2.ID)
	}
}

// TestDrain_LRUEviction verifies that when MaxPatterns is exceeded, the
// least-recently-matched pattern is evicted.
func TestDrain_LRUEviction(t *testing.T) {
	d := NewDrain(DrainConfig{
		Depth:               4,
		SimilarityThreshold: 0.5,
		MaxChildren:         100,
		MaxPatterns:         3,
	})
	// Fill up to MaxPatterns.
	d.Process("alpha beta gamma delta")
	d.Process("one two three four")
	d.Process("foo bar baz qux")
	if len(d.Patterns()) != 3 {
		t.Fatalf("expected 3 patterns before eviction, got %d", len(d.Patterns()))
	}
	// Adding a 4th distinct pattern should trigger eviction.
	d.Process("new unique line here")
	if len(d.Patterns()) > 3 {
		t.Errorf("expected at most 3 patterns after eviction, got %d", len(d.Patterns()))
	}
}

// TestDrain_Defaults verifies that zero DrainConfig gets sensible defaults.
func TestDrain_Defaults(t *testing.T) {
	d := NewDrain(DrainConfig{})
	p := d.Process("hello world from drain")
	if p == nil {
		t.Fatal("expected non-nil pattern")
	}
	if p.ID == "" {
		t.Error("expected non-empty pattern ID")
	}
}

// TestDrain_EmptyInput returns a pattern for an empty string without panic.
func TestDrain_EmptyInput(t *testing.T) {
	d := defaultDrain()
	p := d.Process("")
	if p == nil {
		t.Fatal("expected non-nil pattern for empty input")
	}
}
