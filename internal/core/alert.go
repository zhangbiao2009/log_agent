package core

import (
	"fmt"
	"time"
)

// PatternSummary is a per-pattern rollup within an alert.
type PatternSummary struct {
	Template    string
	Count       int
	Level       string
	SampleLines []string // up to 3 representative lines

	// Set by AnomalyDetector. Zero values when detector is disabled.
	Anomaly  AnomalyKind // what triggered the alert (AnomalyNone = not anomalous)
	Baseline float64     // EMA mean at time of detection (pre-update)
	ZScore   float64     // (Count - mean) / max(stddev, 1.0); pre-update
}

// AnomalyKind classifies why a pattern was flagged, or AnomalyNone for steady state.
type AnomalyKind int

const (
	AnomalyNone       AnomalyKind = iota // 0 — steady state, not anomalous
	AnomalyNewPattern                    // pattern not seen within new_pattern_grace
	AnomalySpike                         // count > mean + spike_multiplier × stddev
	AnomalyRateJump                      // count > rate_jump_factor × mean
)

func (k AnomalyKind) String() string {
	switch k {
	case AnomalyNone:
		return "none"
	case AnomalyNewPattern:
		return "new_pattern"
	case AnomalySpike:
		return "spike"
	case AnomalyRateJump:
		return "rate_jump"
	default:
		return fmt.Sprintf("unknown(%d)", int(k))
	}
}

// Alert is the data sent to notification channels.
type Alert struct {
	Service     string
	Level       string           // highest severity seen (FATAL > ERROR > WARN)
	Count       int              // number of error lines in the window
	Window      time.Duration    // aggregation window
	SampleLines []string         // up to 5 example log lines (Phase 1 fallback)
	Patterns    []PatternSummary // per-pattern breakdown (empty if pattern engine disabled)
	Timestamp   time.Time        // window end time
}
