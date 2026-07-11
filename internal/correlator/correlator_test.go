package correlator

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/core"
	"github.com/zhangbiao2009/log_agent/internal/testutil"
)

// --- helpers ---

func testGraph() *DependencyGraph {
	// A → [B, C], B → [D]
	return NewDependencyGraph(map[string][]string{
		"A": {"B", "C"},
		"B": {"D"},
	})
}

func makeAlert(service string, patterns ...core.PatternSummary) core.Alert {
	return core.Alert{
		Service:   service,
		Level:     "ERROR",
		Count:     10,
		Window:    5 * time.Second,
		Patterns:  patterns,
		Timestamp: time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC),
	}
}

func collectIncidents(t *testing.T, out <-chan core.Incident, n int, timeout time.Duration) []core.Incident {
	t.Helper()
	var result []core.Incident
	timer := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case inc, ok := <-out:
			if !ok {
				return result
			}
			result = append(result, inc)
		case <-timer:
			t.Fatalf("timeout waiting for incident %d/%d", i+1, n)
		}
	}
	return result
}

func drainIncidents(out <-chan core.Incident, timeout time.Duration) []core.Incident {
	var result []core.Incident
	timer := time.After(timeout)
	for {
		select {
		case inc, ok := <-out:
			if !ok {
				return result
			}
			result = append(result, inc)
		case <-timer:
			return result
		}
	}
}

func newTestCorrelator(g *DependencyGraph, clk *testutil.FakeClock) *Correlator {
	c := NewCorrelator(CorrelatorConfig{Window: 5 * time.Second}, g)
	c.Clock = clk
	return c
}

// --- 5.1 Pipeline Mechanics ---

func TestCorrelator_ClosesOutputWhenInputCloses(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	in := make(chan core.Alert)
	out := c.Run(context.Background(), in)
	close(in)
	select {
	case _, ok := <-out:
		if ok {
			// Got an incident from final flush, drain remaining
			for range out {
			}
		}
		// Channel closed — good
	case <-time.After(2 * time.Second):
		t.Fatal("output channel not closed after input closed")
	}
}

func TestCorrelator_ClosesOutputOnContextCancel(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan core.Alert, 1)
	in <- makeAlert("A")
	out := c.Run(ctx, in)
	cancel()
	select {
	case <-out:
		// Drain until closed
		for range out {
		}
	case <-time.After(2 * time.Second):
		t.Fatal("output channel not closed after context cancel")
	}
}

func TestCorrelator_BufferedOutput(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	in := make(chan core.Alert)
	out := c.Run(context.Background(), in)
	close(in)
	for range out {
	}
	// Just verify it ran without deadlock — output buffer is 100.
	if cap(out) != 100 {
		t.Errorf("cap(out) = %d, want 100", cap(out))
	}
}

// --- 5.2 Single-Service Correlation ---

func TestCorrelator_SingleAlertBecomesIncident(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	in := make(chan core.Alert, 1)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A")
	// Let the goroutine pick up the alert.
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	inc := incs[0]
	if len(inc.Services) != 1 || inc.Services[0] != "A" {
		t.Errorf("Services = %v, want [A]", inc.Services)
	}
	if len(inc.Alerts) != 1 {
		t.Errorf("len(Alerts) = %d, want 1", len(inc.Alerts))
	}
}

func TestCorrelator_MultipleAlertsFromSameService(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	in := make(chan core.Alert, 3)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A", core.PatternSummary{Template: "t1", Count: 1, Level: "ERROR"})
	in <- makeAlert("A", core.PatternSummary{Template: "t2", Count: 2, Level: "ERROR"})
	in <- makeAlert("A", core.PatternSummary{Template: "t3", Count: 3, Level: "ERROR"})
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	if len(incs[0].Alerts) != 3 {
		t.Errorf("len(Alerts) = %d, want 3", len(incs[0].Alerts))
	}
	// Verify all 3 alerts are present (not deduplicated).
	templates := make(map[string]bool)
	for _, a := range incs[0].Alerts {
		for _, p := range a.Patterns {
			templates[p.Template] = true
		}
	}
	for _, tmpl := range []string{"t1", "t2", "t3"} {
		if !templates[tmpl] {
			t.Errorf("missing pattern template %s", tmpl)
		}
	}
}

func TestCorrelator_UnknownServiceNotInGraph(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	in := make(chan core.Alert, 1)
	out := c.Run(context.Background(), in)
	in <- makeAlert("X") // X not in graph
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	inc := incs[0]
	if len(inc.DepChain) != 0 {
		t.Errorf("DepChain = %v, want empty", inc.DepChain)
	}
	if inc.RootService != "" {
		t.Errorf("RootService = %q, want empty", inc.RootService)
	}
}

// --- 5.3 Multi-Service Correlation ---

func TestCorrelator_RelatedServicesGrouped(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	g := NewDependencyGraph(map[string][]string{"A": {"B"}, "B": {"D"}})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 2)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A")
	in <- makeAlert("D")
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	if incs[0].RootService != "D" {
		t.Errorf("RootService = %q, want D", incs[0].RootService)
	}
}

func TestCorrelator_UnrelatedServicesNotGrouped(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"C": {"D"},
	})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 2)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A")
	in <- makeAlert("D")
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := drainIncidents(out, 2*time.Second)
	if len(incs) != 2 {
		t.Fatalf("got %d incidents, want 2 (separate components)", len(incs))
	}
}

func TestCorrelator_ThreeServiceIncident(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"B": {"C"},
	})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 3)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A")
	in <- makeAlert("B")
	in <- makeAlert("C")
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	inc := incs[0]
	if inc.RootService != "C" {
		t.Errorf("RootService = %q, want C (depth 2)", inc.RootService)
	}
	if len(inc.DepChain) != 3 || inc.DepChain[0] != "C" || inc.DepChain[1] != "B" || inc.DepChain[2] != "A" {
		t.Errorf("DepChain = %v, want [C B A]", inc.DepChain)
	}
}

func TestCorrelator_TransitiveDependencyGrouped(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"B": {"C"},
	})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 2)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A") // depth 0
	in <- makeAlert("C") // depth 2 (connected through B)
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	if incs[0].RootService != "C" {
		t.Errorf("RootService = %q, want C", incs[0].RootService)
	}
}

func TestCorrelator_DepChainFanOut(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	g := NewDependencyGraph(map[string][]string{"A": {"B", "C"}})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 3)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A")
	in <- makeAlert("B")
	in <- makeAlert("C")
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	dc := incs[0].DepChain
	// B and C at depth 1, A at depth 0. Expected: [B, C, A] (depth desc, then alpha).
	if len(dc) != 3 || dc[0] != "B" || dc[1] != "C" || dc[2] != "A" {
		t.Errorf("DepChain = %v, want [B C A]", dc)
	}
}

// --- 5.4 Root Cause Selection ---

func TestCorrelator_RootCauseIsDeepestService(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	g := NewDependencyGraph(map[string][]string{
		"A": {"B"},
		"B": {"C"},
	})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 3)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A")
	in <- makeAlert("B")
	in <- makeAlert("C")
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if incs[0].RootService != "C" {
		t.Errorf("RootService = %q, want C (deepest)", incs[0].RootService)
	}
}

func TestCorrelator_RootCauseTieBreakByZScore(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	g := NewDependencyGraph(map[string][]string{"A": {"B", "C"}})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 3)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A")
	// B at depth 1, ZScore=5.0
	in <- makeAlert("B", core.PatternSummary{Template: "t1", Count: 100, Level: "ERROR", ZScore: 5.0})
	// C at depth 1, ZScore=2.0
	in <- makeAlert("C", core.PatternSummary{Template: "t2", Count: 50, Level: "ERROR", ZScore: 2.0})
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if incs[0].RootService != "B" {
		t.Errorf("RootService = %q, want B (higher ZScore)", incs[0].RootService)
	}
}

func TestCorrelator_RootCauseSingleService(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	in := make(chan core.Alert, 1)
	out := c.Run(context.Background(), in)
	in <- makeAlert("X") // unknown service
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	incs := collectIncidents(t, out, 1, 2*time.Second)
	if incs[0].RootService != "" {
		t.Errorf("RootService = %q, want empty for single unknown service", incs[0].RootService)
	}
}

// --- 5.5 Window Behavior ---

func TestCorrelator_AlertsInDifferentWindowsSeparated(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	in := make(chan core.Alert, 2)
	out := c.Run(context.Background(), in)
	// Window 1: alert for A.
	in <- makeAlert("A")
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second) // flush window 1
	// Collect first incident.
	inc1 := collectIncidents(t, out, 1, 2*time.Second)
	if len(inc1) != 1 {
		t.Fatalf("window 1: got %d incidents, want 1", len(inc1))
	}
	// Window 2: alert for B.
	in <- makeAlert("B")
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second) // flush window 2
	inc2 := collectIncidents(t, out, 1, 2*time.Second)
	if len(inc2) != 1 {
		t.Fatalf("window 2: got %d incidents, want 1", len(inc2))
	}
}

func TestCorrelator_FlushOnInputClose(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	g := NewDependencyGraph(map[string][]string{"A": {"B"}})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 2)
	out := c.Run(context.Background(), in)
	in <- makeAlert("A")
	in <- makeAlert("B")
	close(in) // close without advancing clock
	incs := drainIncidents(out, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1 (flushed on close)", len(incs))
	}
	if len(incs[0].Services) != 2 {
		t.Errorf("Services = %v, want 2 services", incs[0].Services)
	}
}

func TestCorrelator_EmptyWindowNoIncident(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	c := newTestCorrelator(testGraph(), clk)
	in := make(chan core.Alert)
	out := c.Run(context.Background(), in)
	// Advance past window with no alerts.
	clk.Advance(6 * time.Second)
	// Short wait then close.
	runtime.Gosched()
	time.Sleep(20 * time.Millisecond)
	close(in)
	incs := drainIncidents(out, 2*time.Second)
	if len(incs) != 0 {
		t.Errorf("got %d incidents, want 0 (empty window)", len(incs))
	}
}

// --- Integration Smoke Test ---

func TestCorrelator_EndToEnd_CascadingFailure(t *testing.T) {
	clk := testutil.NewFakeClock(time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC))
	g := NewDependencyGraph(map[string][]string{
		"order-svc":   {"payment-svc"},
		"payment-svc": {"bank-gw"},
	})
	c := newTestCorrelator(g, clk)
	in := make(chan core.Alert, 3)
	out := c.Run(context.Background(), in)

	in <- core.Alert{
		Service:   "bank-gw",
		Level:     "FATAL",
		Count:     0,
		Window:    5 * time.Second,
		Patterns:  []core.PatternSummary{{Template: "service unreachable", Count: 0, Level: "FATAL", Anomaly: core.AnomalyNewPattern}},
		Timestamp: time.Date(2026, 4, 12, 10, 0, 1, 0, time.UTC),
	}
	in <- core.Alert{
		Service:   "payment-svc",
		Level:     "ERROR",
		Count:     200,
		Window:    5 * time.Second,
		Patterns:  []core.PatternSummary{{Template: "connection refused", Count: 200, Level: "ERROR", Anomaly: core.AnomalySpike, ZScore: 12.3}},
		Timestamp: time.Date(2026, 4, 12, 10, 0, 2, 0, time.UTC),
	}
	in <- core.Alert{
		Service:   "order-svc",
		Level:     "ERROR",
		Count:     50,
		Window:    5 * time.Second,
		Patterns:  []core.PatternSummary{{Template: "timeout calling payment", Count: 50, Level: "ERROR", Anomaly: core.AnomalySpike, ZScore: 8.1}},
		Timestamp: time.Date(2026, 4, 12, 10, 0, 3, 0, time.UTC),
	}

	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)
	clk.Advance(6 * time.Second)
	close(in)

	incs := drainIncidents(out, 2*time.Second)
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	inc := incs[0]

	// All 3 services in the incident.
	if len(inc.Services) != 3 {
		t.Errorf("Services = %v, want 3 services", inc.Services)
	}
	// Root cause is bank-gw (depth 2).
	if inc.RootService != "bank-gw" {
		t.Errorf("RootService = %q, want bank-gw", inc.RootService)
	}
	// DepChain: deepest first.
	if len(inc.DepChain) != 3 || inc.DepChain[0] != "bank-gw" || inc.DepChain[1] != "payment-svc" || inc.DepChain[2] != "order-svc" {
		t.Errorf("DepChain = %v, want [bank-gw payment-svc order-svc]", inc.DepChain)
	}
	if len(inc.Alerts) != 3 {
		t.Errorf("len(Alerts) = %d, want 3", len(inc.Alerts))
	}
	// ID is non-empty and deterministic.
	if inc.ID == "" {
		t.Error("ID is empty")
	}
	// Run ID generation again to verify determinism.
	id2 := core.GenerateIncidentID(inc.Services, inc.OpenedAt, inc.Window)
	if inc.ID != id2 {
		t.Errorf("ID not deterministic: %q != %q", inc.ID, id2)
	}
}
