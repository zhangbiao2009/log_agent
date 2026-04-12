package diagnosis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/notify"
)

// DiagnoserConfig controls diagnosis behavior.
type DiagnoserConfig struct {
	Endpoint    string        // LLM API endpoint
	Model       string        // model name (e.g. "deepseek-chat")
	MaxTokens   int           // max response tokens (default: 1024)
	Temperature float64       // 0.0 for deterministic diagnosis
	Timeout     time.Duration // per-call timeout (default: 30s)
}

// Diagnoser enriches incidents with LLM-generated diagnosis.
type Diagnoser struct {
	config DiagnoserConfig
	client LLMClient
}

// NewDiagnoser creates a new Diagnoser.
func NewDiagnoser(cfg DiagnoserConfig, client LLMClient) *Diagnoser {
	return &Diagnoser{
		config: cfg,
		client: client,
	}
}

// Run consumes incidents and emits enriched incidents with diagnosis.
func (d *Diagnoser) Run(ctx context.Context, in <-chan notify.Incident) <-chan notify.Incident {
	out := make(chan notify.Incident, cap(in))
	go func() {
		defer close(out)
		for {
			select {
			case inc, ok := <-in:
				if !ok {
					return
				}
				enriched := d.diagnose(ctx, inc)
				select {
				case out <- enriched:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func (d *Diagnoser) diagnose(ctx context.Context, inc notify.Incident) notify.Incident {
	prompt := BuildPrompt(inc)
	response, err := d.client.Complete(ctx, prompt)
	if err != nil {
		slog.Warn("LLM diagnosis failed, using heuristic", "err", err)
		inc.Diagnosis = fmt.Sprintf("LLM diagnosis unavailable: %v", err)
		inc.Severity = HeuristicSeverity(inc)
		return inc
	}

	severity, diagnosis, suggestions := ParseDiagnosis(response)
	inc.Severity = severity
	inc.Diagnosis = diagnosis
	inc.Suggestions = suggestions

	slog.Info("diagnosis complete",
		"severity", severity,
		"services", inc.Services,
	)
	return inc
}

// HeuristicSeverity assigns severity based on alert data when the LLM is unavailable.
func HeuristicSeverity(inc notify.Incident) string {
	// P1: >=3 services or any FATAL alert.
	if len(inc.Services) >= 3 {
		return "P1"
	}
	for _, a := range inc.Alerts {
		if a.Level == "FATAL" {
			return "P1"
		}
	}

	// P2: 2 services or >=5 spike patterns.
	if len(inc.Services) >= 2 {
		return "P2"
	}
	spikeCount := 0
	for _, a := range inc.Alerts {
		for _, p := range a.Patterns {
			if p.Anomaly == notify.AnomalySpike {
				spikeCount++
			}
		}
	}
	if spikeCount >= 5 {
		return "P2"
	}

	return "P3"
}
