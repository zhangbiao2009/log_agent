package anomaly

import (
	"context"
	"testing"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/notify"
)

// --- helpers ---

func makeAlert(service string, patterns ...notify.PatternSummary) notify.Alert {
	total := 0
	for _, p := range patterns {
		total += p.Count
	}
	return notify.Alert{
		Service:   service,
		Level:     "ERROR",
		Count:     total,
		Window:    1 * time.Minute,
		Patterns:  patterns,
		Timestamp: time.Now(),
	}
}

func collectAlerts(out <-chan notify.Alert, n int, timeout time.Duration) []notify.Alert {
	var result []notify.Alert
	timer := time.After(timeout)
	for len(result) < n {
		select {
		case a, ok := <-out:
			if !ok {
				return result
			}
			result = append(result, a)
		case <-timer:
			return result
		}
	}
	return result
}

func drainAll(out <-chan notify.Alert, timeout time.Duration) []notify.Alert {
	var result []notify.Alert
	timer := time.After(timeout)
	for {
		select {
		case a, ok := <-out:
			if !ok {
				return result
			}
			result = append(result, a)
		case <-timer:
			return result
		}
	}
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time                         { return c.now }
func (c fixedClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func newTestDetector(cfg AnomalyConfig, store BaselineStore, now time.Time) *AnomalyDetector {
	d := NewAnomalyDetector(cfg, store)
	d.Clock = fixedClock{now: now}
	return d
}

func defaultTestConfig() AnomalyConfig {
	return AnomalyConfig{
		SpikeMultiplier: 3.0,
		RateJumpFactor:  5.0,
		EMAAlpha:        0.3,
		MinSamples:      5,
		NewPatternGrace: 24 * time.Hour,
	}
}

// warmup sends n windows with the given count through the detector pipeline,
// draining all output. Returns the store state after warmup.
func warmup(t *testing.T, d *AnomalyDetector, patternTemplate string, count, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		in := make(chan notify.Alert, 1)
		ctx, cancel := context.WithCancel(context.Background())
		out := d.Run(ctx, in)
		in <- makeAlert("svc", notify.PatternSummary{
			Template: patternTemplate,
			Count:    count,
			Level:    "ERROR",
		})
		close(in)
		drainAll(out, 1*time.Second)
		cancel()
	}
}

// --- 5.1 Pipeline Mechanics ---

func TestDetector_ClosesOutputWhenInputCloses(t *testing.T) {
	d := newTestDetector(defaultTestConfig(), NewMemoryStore(), time.Now())
	in := make(chan notify.Alert)
	ctx := context.Background()
	out := d.Run(ctx, in)
	close(in)

	select {
	case _, ok := <-out:
		if ok {
			// Got an alert, keep draining
			for range out {
			}
		}
		// channel closed — good
	case <-time.After(2 * time.Second):
		t.Fatal("output channel not closed after input closed")
	}
}

func TestDetector_ClosesOutputOnContextCancel(t *testing.T) {
	d := newTestDetector(defaultTestConfig(), NewMemoryStore(), time.Now())
	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{Template: "pat1", Count: 1, Level: "ERROR"})
	cancel()

	timer := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return // closed — success
			}
		case <-timer:
			t.Fatal("output channel not closed after context cancel")
		}
	}
}

func TestDetector_BufferedOutputSameCapAsInput(t *testing.T) {
	d := newTestDetector(defaultTestConfig(), NewMemoryStore(), time.Now())
	in := make(chan notify.Alert, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)
	if cap(out) != cap(in) {
		t.Errorf("cap(out)=%d, want %d", cap(out), cap(in))
	}
}

// --- 5.2 Suppression ---

func TestDetector_SuppressesSteadyStateAlert(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	// Warm up 10 windows — all forwarded (first as NewPattern, rest as they are)
	warmup(t, d, "error connecting to <*>", 32, 10)

	// 11th window with same count → should be suppressed
	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{
		Template: "error connecting to <*>",
		Count:    32,
		Level:    "ERROR",
	})
	close(in)

	alerts := drainAll(out, 1*time.Second)
	if len(alerts) != 0 {
		t.Errorf("expected 0 alerts (suppressed), got %d", len(alerts))
	}
}

func TestDetector_ForwardsAlertIfAnyPatternAnomalous(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	// Warm up pat1 and pat2 so they are established
	warmup(t, d, "pat1", 10, 10)
	warmup(t, d, "pat2", 10, 10)

	// Alert with pat1 (steady), pat2 (steady), pat3 (new)
	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc",
		notify.PatternSummary{Template: "pat1", Count: 10, Level: "ERROR"},
		notify.PatternSummary{Template: "pat2", Count: 10, Level: "ERROR"},
		notify.PatternSummary{Template: "pat3", Count: 1, Level: "ERROR"}, // new
	)
	close(in)

	alerts := drainAll(out, 1*time.Second)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert forwarded, got %d", len(alerts))
	}
	a := alerts[0]
	if a.Patterns[2].Anomaly != notify.AnomalyNewPattern {
		t.Errorf("pat3 anomaly = %v, want AnomalyNewPattern", a.Patterns[2].Anomaly)
	}
}

// --- 5.3 NewPattern trigger ---

func TestDetector_FirstObservationIsNewPattern(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{Template: "never-seen", Count: 5, Level: "ERROR"})
	close(in)

	alerts := drainAll(out, 1*time.Second)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	if alerts[0].Patterns[0].Anomaly != notify.AnomalyNewPattern {
		t.Errorf("anomaly = %v, want AnomalyNewPattern", alerts[0].Patterns[0].Anomaly)
	}
}

func TestDetector_PatternReappearsAfterGrace(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()

	// Pre-seed: pattern was seen 25h ago
	store.Set("old-pattern", PatternBaseline{
		N:        10,
		Mean:     5,
		LastSeen: now.Add(-25 * time.Hour),
	})

	d := newTestDetector(defaultTestConfig(), store, now)

	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{Template: "old-pattern", Count: 5, Level: "ERROR"})
	close(in)

	alerts := drainAll(out, 1*time.Second)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	if alerts[0].Patterns[0].Anomaly != notify.AnomalyNewPattern {
		t.Errorf("anomaly = %v, want AnomalyNewPattern", alerts[0].Patterns[0].Anomaly)
	}
}

// --- 5.4 Spike trigger ---

func TestDetector_SpikeAnnotatedOnPatternSummary(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	// Warm up 10 windows at count=32
	warmup(t, d, "spike-test", 32, 10)

	// Send count=200 → should spike
	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{Template: "spike-test", Count: 200, Level: "ERROR"})
	close(in)

	alerts := drainAll(out, 1*time.Second)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	ps := alerts[0].Patterns[0]
	if ps.Anomaly != notify.AnomalySpike {
		t.Errorf("anomaly = %v, want AnomalySpike", ps.Anomaly)
	}
	if ps.ZScore <= 3.0 {
		t.Errorf("ZScore = %f, want > 3.0", ps.ZScore)
	}
	// Baseline should reflect pre-update mean ≈ 32 (not shifted by 200)
	if ps.Baseline < 28 || ps.Baseline > 36 {
		t.Errorf("Baseline = %f, want ≈32", ps.Baseline)
	}
}

func TestDetector_NoSpikeBeforeMinSamples(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	cfg := defaultTestConfig()
	cfg.MinSamples = 5
	d := newTestDetector(cfg, store, now)

	// Send only 3 windows at count=32 (below minSamples=5)
	warmup(t, d, "warmup-pat", 32, 3)

	// Send count=200 — should NOT be spike (only NewPattern on first window)
	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{Template: "warmup-pat", Count: 200, Level: "ERROR"})
	close(in)

	alerts := drainAll(out, 1*time.Second)
	// The 4th window — pattern was seen recently, N=3 < minSamples=5
	// So not NewPattern (since LastSeen is recent) and not Spike (N<5)
	// → should be suppressed
	for _, a := range alerts {
		for _, ps := range a.Patterns {
			if ps.Anomaly == notify.AnomalySpike {
				t.Errorf("unexpected spike before minSamples: %+v", ps)
			}
		}
	}
}

// --- 5.5 RateJump trigger ---

func TestDetector_RateJumpDetected(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	cfg := defaultTestConfig()
	d := newTestDetector(cfg, store, now)

	// Pre-seed baseline: Mean=10, Stddev=2 (Var=4), N=10, recently seen.
	// Spike threshold = 10 + 3*2 = 16. RateJump threshold = 5*10 = 50.
	// Count=55 → above RateJump (55 > 50) AND above Spike (55 > 16).
	// But Spike wins by priority. So we need a count that triggers RateJump
	// but NOT Spike. That's impossible with normal params when stddev is small.
	// Instead, disable spike with a very high multiplier.
	cfg.SpikeMultiplier = 1000.0 // effectively disable spike
	// Also pre-seed with known variance so threshold is well-defined.
	store.Set("ratejump-pat", PatternBaseline{
		N:        10,
		Mean:     10,
		Variance: 4, // Stddev=2, spike threshold = 10 + 1000*2 = 2010
		LastSeen: now.Add(-1 * time.Minute),
	})
	d = newTestDetector(cfg, store, now)

	// Send count=55 (5.5× mean, above factor=5)
	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{Template: "ratejump-pat", Count: 55, Level: "ERROR"})
	close(in)

	alerts := drainAll(out, 1*time.Second)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	ps := alerts[0].Patterns[0]
	if ps.Anomaly != notify.AnomalyRateJump {
		t.Errorf("anomaly = %v, want AnomalyRateJump", ps.Anomaly)
	}
}

func TestDetector_NoRateJumpBelowFactor(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	// Warm up 10 windows at count=10
	warmup(t, d, "noratejump-pat", 10, 10)

	// Send count=30 (3× mean, below factor=5)
	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{Template: "noratejump-pat", Count: 30, Level: "ERROR"})
	close(in)

	alerts := drainAll(out, 1*time.Second)
	// Could be spike or suppressed. Should NOT be RateJump.
	for _, a := range alerts {
		for _, ps := range a.Patterns {
			if ps.Anomaly == notify.AnomalyRateJump {
				t.Errorf("unexpected RateJump for count=30 with mean=10")
			}
		}
	}
}

// --- 5.6 Baseline updates ---

func TestDetector_BaselineIsUpdatedAfterEachWindow(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	warmup(t, d, "update-test", 50, 5)

	b, ok := store.Get("update-test")
	if !ok {
		t.Fatal("expected baseline in store")
	}
	// After 5 updates of 50, Mean should be close to 50
	if b.Mean < 35 || b.Mean > 55 {
		t.Errorf("Mean = %f, want ≈50", b.Mean)
	}
}

func TestDetector_BaselineUpdatedForSuppressedWindows(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	// 10 windows → baselines established (all forwarded: first as NewPattern)
	warmup(t, d, "suppressed-update", 32, 10)

	// 5 more windows → should be suppressed (AnomalyNone)
	warmup(t, d, "suppressed-update", 32, 5)

	b, ok := store.Get("suppressed-update")
	if !ok {
		t.Fatal("expected baseline in store")
	}
	// N should be 15 (10 + 5), proving suppressed windows still called Update
	if b.N != 15 {
		t.Errorf("N = %d, want 15 (suppressed windows must update baseline)", b.N)
	}
}

func TestDetector_SpikeWinsOverRateJump(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	// Warm up 10 windows at count=10
	warmup(t, d, "priority-test", 10, 10)

	// Send count=200: satisfies both Spike (200 > 10+3*stddev) AND RateJump (200 > 5*10=50)
	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	in <- makeAlert("svc", notify.PatternSummary{Template: "priority-test", Count: 200, Level: "ERROR"})
	close(in)

	alerts := drainAll(out, 1*time.Second)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	ps := alerts[0].Patterns[0]
	if ps.Anomaly != notify.AnomalySpike {
		t.Errorf("anomaly = %v, want AnomalySpike (Spike > RateJump priority)", ps.Anomaly)
	}
}

func TestDetector_Phase1AlertsForwardedAsIs(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	d := newTestDetector(defaultTestConfig(), store, now)

	in := make(chan notify.Alert, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	// Phase 1 alert: no patterns
	in <- notify.Alert{
		Service:     "svc",
		Level:       "ERROR",
		Count:       5,
		Window:      1 * time.Minute,
		SampleLines: []string{"error line 1", "error line 2"},
	}
	close(in)

	alerts := drainAll(out, 1*time.Second)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert forwarded as-is, got %d", len(alerts))
	}
	if len(alerts[0].Patterns) != 0 {
		t.Errorf("expected no patterns, got %d", len(alerts[0].Patterns))
	}
}

// --- Integration smoke test ---

func TestDetector_EndToEnd_SteadyThenSpike(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	cfg := AnomalyConfig{
		SpikeMultiplier: 3.0,
		EMAAlpha:        0.3,
		MinSamples:      5,
		NewPatternGrace: 24 * time.Hour,
		RateJumpFactor:  5.0,
	}
	d := newTestDetector(cfg, store, now)

	pat := "failed to process event <*>"

	// Phase 1: warm up 20 windows
	warmup(t, d, pat, 32, 20)

	// Phase 2: 5 more steady windows → all suppressed
	in := make(chan notify.Alert, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := d.Run(ctx, in)

	for i := 0; i < 5; i++ {
		in <- makeAlert("svc", notify.PatternSummary{Template: pat, Count: 32, Level: "ERROR"})
	}
	// 1 spike
	in <- makeAlert("svc", notify.PatternSummary{Template: pat, Count: 200, Level: "ERROR"})
	// 3 more steady
	for i := 0; i < 3; i++ {
		in <- makeAlert("svc", notify.PatternSummary{Template: pat, Count: 32, Level: "ERROR"})
	}
	close(in)

	alerts := drainAll(out, 2*time.Second)
	if len(alerts) != 1 {
		t.Fatalf("expected exactly 1 spike alert, got %d", len(alerts))
	}
	ps := alerts[0].Patterns[0]
	if ps.Anomaly != notify.AnomalySpike {
		t.Errorf("anomaly = %v, want AnomalySpike", ps.Anomaly)
	}
	if ps.ZScore <= 3.0 {
		t.Errorf("ZScore = %f, want > 3.0", ps.ZScore)
	}
}

// --- Benchmark ---

func BenchmarkDetector_Ingest(b *testing.B) {
	store := NewMemoryStore()
	cfg := defaultTestConfig()
	d := NewAnomalyDetector(cfg, store)

	// Pre-warm 1000 pattern baselines
	for i := 0; i < 1000; i++ {
		pat := "pattern-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26%10))
		store.Set(pat, PatternBaseline{N: 20, Mean: 50, Variance: 25})
	}

	// Build a representative alert with 10 patterns
	patterns := make([]notify.PatternSummary, 10)
	for i := range patterns {
		patterns[i] = notify.PatternSummary{
			Template: "pattern-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26%10)),
			Count:    50 + i,
			Level:    "ERROR",
		}
	}
	alert := notify.Alert{
		Service:  "bench-svc",
		Level:    "ERROR",
		Count:    500,
		Window:   1 * time.Minute,
		Patterns: patterns,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.evaluate(alert)
	}
}
