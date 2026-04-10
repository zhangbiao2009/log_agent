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
