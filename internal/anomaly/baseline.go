package anomaly

import (
	"math"
	"time"
)

// PatternBaseline holds the EMA statistics for a single pattern.
// It is updated once per aggregation window.
type PatternBaseline struct {
	N        int       // number of observations so far
	Mean     float64   // EMA mean
	Variance float64   // EMA variance
	LastSeen time.Time // last window this pattern was observed (zero = never seen)
}

// Update incorporates a new window count into the EMA.
func (b *PatternBaseline) Update(count int, alpha float64) {
	c := float64(count)
	if b.N == 0 {
		b.Mean = c
		b.Variance = 0
	} else {
		delta := c - b.Mean
		b.Mean += alpha * delta
		b.Variance = (1 - alpha) * (b.Variance + alpha*delta*delta)
	}
	b.N++
}

// Stddev returns sqrt(Variance), floored at 0.
func (b *PatternBaseline) Stddev() float64 {
	if b.Variance <= 0 {
		return 0
	}
	return math.Sqrt(b.Variance)
}

// IsNewPattern returns true if this pattern has not been seen within grace.
// A zero LastSeen (never observed) gives time.Since ≈ 56 years, which is
// always > any reasonable grace, so unseen patterns always return true.
func (b *PatternBaseline) IsNewPattern(grace time.Duration, now time.Time) bool {
	return now.Sub(b.LastSeen) > grace
}

// IsSpike returns true if count exceeds mean + multiplier×stddev.
// Returns false until minSamples windows have been observed (warmup guard).
func (b *PatternBaseline) IsSpike(count int, multiplier float64, minSamples int) bool {
	if b.N < minSamples {
		return false
	}
	return float64(count) > b.Mean+multiplier*b.Stddev()
}

// IsRateJump returns true if count exceeds factor×mean.
// Returns false until minSamples windows have been observed, or when mean == 0.
func (b *PatternBaseline) IsRateJump(count int, factor float64, minSamples int) bool {
	if b.N < minSamples || b.Mean == 0 {
		return false
	}
	return float64(count) > factor*b.Mean
}

// ZScore returns (count - mean) / max(stddev, 1.0).
func (b *PatternBaseline) ZScore(count int) float64 {
	stddev := b.Stddev()
	if stddev < 1.0 {
		stddev = 1.0
	}
	return (float64(count) - b.Mean) / stddev
}
