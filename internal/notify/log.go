package notify

import (
	"context"
	"fmt"
	"github.com/zhangbiao2009/log_agent/internal/core"
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

func (l *LogNotifier) Send(_ context.Context, incident core.Incident) error {
	if incident.IsSingleAlert() {
		l.sendAlert(incident.Alerts[0])
		if incident.Diagnosis != "" {
			l.sendDiagnosis(incident)
		}
		return nil
	}

	// Multi-alert incident header.
	attrs := []any{
		"id", incident.ID,
		"root", incident.RootService,
	}
	if incident.Severity != "" {
		attrs = append(attrs, "severity", incident.Severity)
	}
	if incident.EventType != "" {
		attrs = append(attrs, "event", incident.EventType, "status", string(incident.Status))
	}
	if incident.Duration > 0 {
		attrs = append(attrs, "duration", incident.Duration.String())
	}
	attrs = append(attrs,
		"services", strings.Join(incident.Services, ", "),
		"chain", strings.Join(incident.DepChain, " → "),
	)
	l.Logger.Info("INCIDENT", attrs...)

	if incident.Diagnosis != "" {
		l.sendDiagnosis(incident)
	}

	for _, alert := range incident.Alerts {
		l.sendAlert(alert)
	}
	return nil
}

func (l *LogNotifier) sendDiagnosis(inc core.Incident) {
	l.Logger.Info("DIAGNOSIS",
		"severity", inc.Severity,
		"diagnosis", inc.Diagnosis,
	)
	if len(inc.Suggestions) > 0 {
		var sb strings.Builder
		for i, s := range inc.Suggestions {
			sb.WriteString(fmt.Sprintf("\n  %d. %s", i+1, s))
		}
		l.Logger.Info("SUGGESTIONS", "actions", sb.String())
	}
}

func (l *LogNotifier) sendAlert(alert core.Alert) {
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
		return
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
}

// anomalyTag returns the annotation suffix for a core.PatternSummary, e.g. " SPIKE z=4.2".
// Returns an empty string for core.AnomalyNone.
func anomalyTag(p core.PatternSummary) string {
	switch p.Anomaly {
	case core.AnomalyNewPattern:
		return " NEW"
	case core.AnomalySpike:
		return fmt.Sprintf(" SPIKE z=%.1f", p.ZScore)
	case core.AnomalyRateJump:
		return fmt.Sprintf(" RATE-JUMP z=%.1f", p.ZScore)
	default:
		return ""
	}
}
