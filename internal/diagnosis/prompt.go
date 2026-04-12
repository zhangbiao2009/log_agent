package diagnosis

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zhangbiao2009/log_agent/internal/notify"
)

const maxPromptServices = 5

// BuildPrompt assembles the LLM prompt from incident data.
func BuildPrompt(inc notify.Incident) string {
	var sb strings.Builder

	sb.WriteString("You are an SRE assistant diagnosing a production incident.\n\n")
	sb.WriteString("INCIDENT CONTEXT:\n")
	sb.WriteString(fmt.Sprintf("- Time: %s\n", inc.OpenedAt.UTC().Format("2006-01-02 15:04:05 UTC")))
	sb.WriteString(fmt.Sprintf("- Affected services: %s\n", strings.Join(inc.Services, ", ")))

	if len(inc.DepChain) > 0 {
		sb.WriteString(fmt.Sprintf("- Dependency chain: %s\n", strings.Join(inc.DepChain, " → ")))
	}
	if inc.RootService != "" {
		sb.WriteString(fmt.Sprintf("- Suspected root cause: %s\n", inc.RootService))
	}

	sb.WriteString("\nLOG PATTERNS (per service):\n")

	alerts := inc.Alerts
	omitted := 0
	if len(alerts) > maxPromptServices {
		// Keep the top services by max ZScore.
		type scored struct {
			alert notify.Alert
			z     float64
		}
		items := make([]scored, len(alerts))
		for i, a := range alerts {
			items[i] = scored{alert: a, z: maxAlertZScore(a)}
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].z > items[j].z
		})
		omitted = len(items) - maxPromptServices
		kept := make([]notify.Alert, maxPromptServices)
		for i := 0; i < maxPromptServices; i++ {
			kept[i] = items[i].alert
		}
		alerts = kept
	}

	for _, a := range alerts {
		sb.WriteString(fmt.Sprintf("\n[%s] — %d errors in %s (%s)\n", a.Service, a.Count, a.Window, a.Level))
		for _, p := range a.Patterns {
			tag := promptAnomalyTag(p)
			sb.WriteString(fmt.Sprintf("  Pattern: \"%s\" (%dx)%s\n", p.Template, p.Count, tag))
			for _, line := range p.SampleLines {
				sb.WriteString(fmt.Sprintf("    \"%s\"\n", line))
			}
		}
	}

	if omitted > 0 {
		sb.WriteString(fmt.Sprintf("\n(%d additional services omitted)\n", omitted))
	}

	sb.WriteString(`
Based on the above, respond in EXACTLY this format:

SEVERITY: P1 | P2 | P3
DIAGNOSIS: <one paragraph explaining the root cause>
SUGGESTIONS:
- <action 1>
- <action 2>
- <action 3>
`)

	return sb.String()
}

func maxAlertZScore(a notify.Alert) float64 {
	var max float64
	for _, p := range a.Patterns {
		if p.ZScore > max {
			max = p.ZScore
		}
	}
	return max
}

func promptAnomalyTag(p notify.PatternSummary) string {
	switch p.Anomaly {
	case notify.AnomalyNewPattern:
		return " [NEW]"
	case notify.AnomalySpike:
		return fmt.Sprintf(" [SPIKE z=%.1f]", p.ZScore)
	case notify.AnomalyRateJump:
		return fmt.Sprintf(" [RATE-JUMP z=%.1f]", p.ZScore)
	default:
		return ""
	}
}
