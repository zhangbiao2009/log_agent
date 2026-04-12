package anomaly

import (
	"math"
	"testing"
	"time"
)

// --- 3.1 Update / EMA Convergence ---

func TestBaseline_FirstUpdate(t *testing.T) {
	var b PatternBaseline
	b.Update(50, 0.3)
	if b.Mean != 50 {
		t.Errorf("Mean = %f, want 50", b.Mean)
	}
	if b.Variance != 0 {
		t.Errorf("Variance = %f, want 0", b.Variance)
	}
	if b.N != 1 {
		t.Errorf("N = %d, want 1", b.N)
	}
}

func TestBaseline_SecondUpdate(t *testing.T) {
	var b PatternBaseline
	b.Update(50, 0.3) // Mean=50, Var=0, N=1
	b.Update(60, 0.3)

	// delta = 60-50 = 10
	// Mean = 50 + 0.3*10 = 53
	wantMean := 53.0
	if math.Abs(b.Mean-wantMean) > 1e-9 {
		t.Errorf("Mean = %f, want %f", b.Mean, wantMean)
	}
	// Variance = (1-0.3)*(0 + 0.3*100) = 0.7*30 = 21
	wantVar := 21.0
	if math.Abs(b.Variance-wantVar) > 1e-9 {
		t.Errorf("Variance = %f, want %f", b.Variance, wantVar)
	}
	if b.N != 2 {
		t.Errorf("N = %d, want 2", b.N)
	}
}

func TestBaseline_ConvergesOnSteadyInput(t *testing.T) {
	var b PatternBaseline
	for i := 0; i < 30; i++ {
		b.Update(32, 0.3)
	}
	if math.Abs(b.Mean-32) > 0.1 {
		t.Errorf("Mean = %f, want ≈32", b.Mean)
	}
	if b.Variance > 0.5 {
		t.Errorf("Variance = %f, want < 0.5", b.Variance)
	}
}

func TestBaseline_AdaptsAfterShift(t *testing.T) {
	var b PatternBaseline
	for i := 0; i < 10; i++ {
		b.Update(30, 0.3)
	}
	for i := 0; i < 10; i++ {
		b.Update(100, 0.3)
	}
	if b.Mean < 70 {
		t.Errorf("Mean = %f, want > 70 after shift to 100", b.Mean)
	}
}

// --- 3.2 Stddev ---

func TestBaseline_StddevZeroOnFirstObservation(t *testing.T) {
	var b PatternBaseline
	b.Update(50, 0.3)
	if b.Stddev() != 0 {
		t.Errorf("Stddev = %f, want 0", b.Stddev())
	}
}

func TestBaseline_StddevPositiveAfterVariance(t *testing.T) {
	var b PatternBaseline
	b.Update(10, 0.3)
	b.Update(50, 0.3)
	if b.Stddev() <= 0 {
		t.Errorf("Stddev = %f, want > 0", b.Stddev())
	}
}

// --- 3.3 IsNewPattern ---

func TestBaseline_NewPatternOnFirstObservation(t *testing.T) {
	var b PatternBaseline // LastSeen is zero
	now := time.Now()
	if !b.IsNewPattern(24*time.Hour, now) {
		t.Error("expected IsNewPattern=true for zero LastSeen")
	}
}

func TestBaseline_NewPatternAfterGraceExpired(t *testing.T) {
	now := time.Now()
	b := PatternBaseline{LastSeen: now.Add(-25 * time.Hour)}
	if !b.IsNewPattern(24*time.Hour, now) {
		t.Error("expected IsNewPattern=true after grace expired")
	}
}

func TestBaseline_NotNewPatternWithinGrace(t *testing.T) {
	now := time.Now()
	b := PatternBaseline{LastSeen: now.Add(-1 * time.Hour)}
	if b.IsNewPattern(24*time.Hour, now) {
		t.Error("expected IsNewPattern=false within grace")
	}
}

func TestBaseline_NotNewPatternExactlyAtGraceBoundary(t *testing.T) {
	now := time.Now()
	b := PatternBaseline{LastSeen: now.Add(-24 * time.Hour)}
	// Condition is >, not >= : 24h > 24h = false
	if b.IsNewPattern(24*time.Hour, now) {
		t.Error("expected IsNewPattern=false at exact grace boundary")
	}
}

// --- 3.4 IsSpike ---

func TestBaseline_NoSpikeBeforeMinSamples(t *testing.T) {
	b := PatternBaseline{N: 3, Mean: 10, Variance: 4}
	if b.IsSpike(1000, 3.0, 5) {
		t.Error("expected no spike before minSamples")
	}
}

func TestBaseline_NoSpikeAtExactThreshold(t *testing.T) {
	var b PatternBaseline
	for i := 0; i < 10; i++ {
		b.Update(32, 0.3)
	}
	threshold := b.Mean + 3.0*b.Stddev()
	count := int(threshold)
	if b.IsSpike(count, 3.0, 5) {
		t.Errorf("expected no spike at count=%d (threshold=%f)", count, threshold)
	}
}

func TestBaseline_SpikeJustAboveThreshold(t *testing.T) {
	var b PatternBaseline
	for i := 0; i < 10; i++ {
		b.Update(32, 0.3)
	}
	threshold := b.Mean + 3.0*b.Stddev()
	count := int(threshold) + 1
	if !b.IsSpike(count, 3.0, 5) {
		t.Errorf("expected spike at count=%d (threshold=%f)", count, threshold)
	}
}

func TestBaseline_SpikeOnHighCount(t *testing.T) {
	var b PatternBaseline
	for i := 0; i < 10; i++ {
		b.Update(32, 0.3)
	}
	if !b.IsSpike(200, 3.0, 5) {
		t.Error("expected spike on high count=200")
	}
}

func TestBaseline_NoSpikeOnSteadyBaseline(t *testing.T) {
	var b PatternBaseline
	for i := 0; i < 20; i++ {
		b.Update(32, 0.3)
	}
	// After convergence on constant input, stddev ≈ 0 so threshold ≈ mean.
	// Use count = 32 (equal to mean) — should never spike.
	if b.IsSpike(32, 3.0, 5) {
		t.Error("expected no spike on count=32 in steady baseline")
	}
}

// --- 3.5 IsRateJump ---

func TestBaseline_NoRateJumpBeforeMinSamples(t *testing.T) {
	b := PatternBaseline{N: 2, Mean: 10}
	if b.IsRateJump(1000, 5.0, 5) {
		t.Error("expected no rate jump before minSamples")
	}
}

func TestBaseline_NoRateJumpWhenMeanIsZero(t *testing.T) {
	b := PatternBaseline{N: 10, Mean: 0}
	if b.IsRateJump(10, 5.0, 5) {
		t.Error("expected no rate jump when mean=0")
	}
}

func TestBaseline_RateJumpDetected(t *testing.T) {
	b := PatternBaseline{N: 10, Mean: 2.0}
	if !b.IsRateJump(11, 5.0, 5) {
		t.Error("expected rate jump: 11 > 5*2=10")
	}
}

func TestBaseline_NoRateJumpBelowFactor(t *testing.T) {
	b := PatternBaseline{N: 10, Mean: 10.0}
	if b.IsRateJump(49, 5.0, 5) {
		t.Error("expected no rate jump: 49 < 5*10=50")
	}
}

// --- 3.6 ZScore ---

func TestBaseline_ZScoreFlooredAtStddevOne(t *testing.T) {
	b := PatternBaseline{N: 1, Mean: 10, Variance: 0}
	got := b.ZScore(100)
	want := 90.0
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("ZScore = %f, want %f", got, want)
	}
}

func TestBaseline_ZScoreNegativeWhenBelowMean(t *testing.T) {
	b := PatternBaseline{N: 10, Mean: 50, Variance: 25}
	got := b.ZScore(35)
	want := -3.0
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("ZScore = %f, want %f", got, want)
	}
}
