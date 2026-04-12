package anomaly

import (
	"context"
	"log/slog"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/notify"
)

// AnomalyConfig controls the anomaly detection thresholds.
// Zero values are replaced by safe defaults in NewAnomalyDetector.
type AnomalyConfig struct {
	SpikeMultiplier float64       // default: 3.0
	RateJumpFactor  float64       // default: 5.0
	EMAAlpha        float64       // default: 0.3
	MinSamples      int           // default: 5
	NewPatternGrace time.Duration // default: 24h
}

func (c *AnomalyConfig) setDefaults() {
	if c.SpikeMultiplier == 0 {
		c.SpikeMultiplier = 3.0
	}
	if c.RateJumpFactor == 0 {
		c.RateJumpFactor = 5.0
	}
	if c.EMAAlpha == 0 {
		c.EMAAlpha = 0.3
	}
	if c.MinSamples == 0 {
		c.MinSamples = 5
	}
	if c.NewPatternGrace == 0 {
		c.NewPatternGrace = 24 * time.Hour
	}
}

// AnomalyDetector is a channel pipeline stage that sits between the Aggregator
// and the Dispatcher. It annotates each PatternSummary with anomaly information
// and forwards only alerts where at least one pattern is anomalous.
//
// Alerts with no PatternSummary entries (pattern engine disabled / Phase 1
// mode) are forwarded as-is without modification.
type AnomalyDetector struct {
	config AnomalyConfig
	store  BaselineStore
	Clock  notify.Clock // exported for test injection; defaults to real clock
}

func NewAnomalyDetector(cfg AnomalyConfig, store BaselineStore) *AnomalyDetector {
	cfg.setDefaults()
	return &AnomalyDetector{
		config: cfg,
		store:  store,
		Clock:  realClock{},
	}
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Run consumes Alerts from in, annotates PatternSummary entries, and emits
// only anomalous alerts to the returned channel.
// The output channel is closed when ctx is done or in is closed.
func (d *AnomalyDetector) Run(ctx context.Context, in <-chan notify.Alert) <-chan notify.Alert {
	out := make(chan notify.Alert, cap(in))
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case alert, ok := <-in:
				if !ok {
					return
				}
				annotated, hasAnomaly := d.evaluate(alert)
				if !hasAnomaly {
					continue
				}
				select {
				case out <- annotated:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// evaluate annotates all PatternSummary entries and reports whether any are anomalous.
// Alerts with no patterns (Phase 1 mode) are returned unchanged with hasAnomaly=true
// so they pass through to the dispatcher.
func (d *AnomalyDetector) evaluate(alert notify.Alert) (notify.Alert, bool) {
	if len(alert.Patterns) == 0 {
		return alert, true
	}

	now := d.Clock.Now()
	hasAnomaly := false

	for i, ps := range alert.Patterns {
		patternID := ps.Template // use template as ID when PatternID isn't on PatternSummary
		// PatternID lives on LogLine, not PatternSummary. The template is a
		// stable proxy because it's deterministic for the same log shape.
		baseline, _ := d.store.Get(patternID)

		// Snapshot pre-update stats so reported Baseline/ZScore reflect
		// what the system compared against, not the post-update mean.
		preMean := baseline.Mean
		preStddev := baseline.Stddev()

		// Classify using pre-update baseline.
		var kind notify.AnomalyKind
		switch {
		case baseline.IsNewPattern(d.config.NewPatternGrace, now):
			kind = notify.AnomalyNewPattern
		case baseline.IsSpike(ps.Count, d.config.SpikeMultiplier, d.config.MinSamples):
			kind = notify.AnomalySpike
		case baseline.IsRateJump(ps.Count, d.config.RateJumpFactor, d.config.MinSamples):
			kind = notify.AnomalyRateJump
		default:
			kind = notify.AnomalyNone
		}

		if kind != notify.AnomalyNone {
			hasAnomaly = true
			slog.Debug("anomaly detected",
				"service", alert.Service,
				"pattern", patternID,
				"kind", kind,
				"count", ps.Count,
				"baseline", preMean,
			)
		}

		// Annotate the summary.
		stddev := preStddev
		if stddev < 1.0 {
			stddev = 1.0
		}
		alert.Patterns[i].Anomaly = kind
		alert.Patterns[i].Baseline = preMean
		alert.Patterns[i].ZScore = (float64(ps.Count) - preMean) / stddev

		// Update baseline AFTER classification and annotation.
		baseline.Update(ps.Count, d.config.EMAAlpha)
		baseline.LastSeen = now
		d.store.Set(patternID, baseline)
	}

	return alert, hasAnomaly
}
