package anomaly

import (
	"testing"
)

func TestMemoryStore_GetMissingKey(t *testing.T) {
	s := NewMemoryStore()
	_, ok := s.Get("nokey")
	if ok {
		t.Error("expected ok=false for missing key")
	}
}

func TestMemoryStore_SetAndGet(t *testing.T) {
	s := NewMemoryStore()
	b := PatternBaseline{N: 5, Mean: 32}
	s.Set("pat1", b)

	got, ok := s.Get("pat1")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.N != 5 || got.Mean != 32 {
		t.Errorf("got %+v, want N=5 Mean=32", got)
	}
}

func TestMemoryStore_OverwritePreviousValue(t *testing.T) {
	s := NewMemoryStore()
	s.Set("pat1", PatternBaseline{N: 1, Mean: 10})
	s.Set("pat1", PatternBaseline{N: 2, Mean: 20})

	got, ok := s.Get("pat1")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.N != 2 || got.Mean != 20 {
		t.Errorf("got %+v, want N=2 Mean=20", got)
	}
}

func TestMemoryStore_IndependentKeys(t *testing.T) {
	s := NewMemoryStore()
	s.Set("pat1", PatternBaseline{N: 1, Mean: 10})
	s.Set("pat2", PatternBaseline{N: 2, Mean: 20})

	g1, _ := s.Get("pat1")
	g2, _ := s.Get("pat2")
	if g1.Mean != 10 || g2.Mean != 20 {
		t.Errorf("pat1=%+v pat2=%+v", g1, g2)
	}
}

func TestMemoryStore_ZeroValueBaseline(t *testing.T) {
	s := NewMemoryStore()
	s.Set("pat1", PatternBaseline{})
	got, ok := s.Get("pat1")
	if !ok {
		t.Fatal("expected ok=true for zero-value baseline")
	}
	if got.N != 0 || got.Mean != 0 {
		t.Errorf("got %+v, want zero value", got)
	}
}
