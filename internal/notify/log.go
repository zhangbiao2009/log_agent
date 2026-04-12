package notify

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

type LogNotifier struct {
	Logger *slog.Logger
}

func NewLogNotifier(logger *slog.Logger) *LogNotifier {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogNotifier{Logger: logger}
}

func (l *LogNotifier) Name() string { return "log" }

func (l *LogNotifier) Send(_ context.Context, alert Alert) error {
	if len(alert.Patterns) > 0 {
		var sb strings.Builder
		for _, p := range alert.Patterns {
			tag := anomalyTag(p)
			sb.WriteString(fmt.Sprintf("\n  [%dx %s%s] %s", p.Count, p.Level, tag, p.Template))
		}
		l.Logger.Info("ALERT",
			"service", alert.Service,
			"level", alert.Level,
			"count", alert.Count,
			"window", alert.Window.String(),
			"patterns", sb.String(),
		)
		return nil
	}

	samples := ""
	if len(alert.SampleLines) > 0 {
		var sb strings.Builder
		for _, s := range alert.SampleLines {
			sb.WriteString(fmt.Sprintf("\n  sample: %q", s))
		}
		samples = sb.String()
	}
	l.Logger.Info("ALERT",
		"service", alert.Service,
		"level", alert.Level,
		"count", alert.Count,
		"window", alert.Window.String(),
		"samples", samples,
	)
	return nil
}

// anomalyTag returns the annotation suffix for a PatternSummary, e.g. " SPIKE z=4.2".
// Returns an empty string for AnomalyNone.
func anomalyTag(p PatternSummary) string {
	switch p.Anomaly {
	case AnomalyNewPattern:
		return " NEW"
	case AnomalySpike:
		return fmt.Sprintf(" SPIKE z=%.1f", p.ZScore)
	case AnomalyRateJump:
		return fmt.Sprintf(" RATE-JUMP z=%.1f", p.ZScore)
	default:
		return ""
	}
}
