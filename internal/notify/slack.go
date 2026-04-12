package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type SlackNotifier struct {
	WebhookURL string
	Client     *http.Client
}

func NewSlackNotifier(webhookURL string) *SlackNotifier {
	return &SlackNotifier{
		WebhookURL: webhookURL,
		Client:     &http.Client{Timeout: 10 * 1e9},
	}
}

func (s *SlackNotifier) Name() string { return "slack" }

func (s *SlackNotifier) Send(ctx context.Context, incident Incident) error {
	var msg slackMessage
	if incident.IsSingleAlert() {
		msg = s.formatAlertMessage(incident.Alerts[0])
		if incident.Diagnosis != "" {
			msg.Blocks = append(msg.Blocks, s.diagnosisBlocks(incident)...)
		}
	} else {
		msg = s.formatIncidentMessage(incident)
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal slack message: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client.Do(req)
	if err != nil {
		return fmt.Errorf("slack webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("slack returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

type slackMessage struct {
	Blocks []slackBlock `json:"blocks"`
}
type slackBlock struct {
	Type string     `json:"type"`
	Text *slackText `json:"text,omitempty"`
}
type slackText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (s *SlackNotifier) formatAlertMessage(alert Alert) slackMessage {
	emoji := levelEmoji(alert.Level)
	header := fmt.Sprintf("%s *%s* — %s (%d errors in last %s)",
		emoji, alert.Level, alert.Service, alert.Count, alert.Window)
	blocks := []slackBlock{
		{Type: "section", Text: &slackText{Type: "mrkdwn", Text: header}},
	}
	if len(alert.Patterns) > 0 {
		for _, p := range alert.Patterns {
			var sb strings.Builder
			badge := anomalyBadge(p)
			sb.WriteString(fmt.Sprintf("%s*[%dx %s]* `%s`\n", badge, p.Count, p.Level, slackEscape(p.Template)))
			for _, line := range p.SampleLines {
				sb.WriteString("• ")
				sb.WriteString(slackEscape(line))
				sb.WriteString("\n")
			}
			blocks = append(blocks, slackBlock{
				Type: "section",
				Text: &slackText{Type: "mrkdwn", Text: sb.String()},
			})
		}
	} else if len(alert.SampleLines) > 0 {
		var sb strings.Builder
		sb.WriteString("*Samples:*\n")
		for _, line := range alert.SampleLines {
			sb.WriteString("• ")
			sb.WriteString(slackEscape(line))
			sb.WriteString("\n")
		}
		blocks = append(blocks, slackBlock{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: sb.String()},
		})
	}
	return slackMessage{Blocks: blocks}
}

func slackEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

func levelEmoji(level string) string {
	switch level {
	case "FATAL":
		return "\U0001F480"
	case "ERROR":
		return "\U0001F534"
	case "WARN":
		return "\U0001F7E1"
	default:
		return "\u2139\uFE0F"
	}
}

func (s *SlackNotifier) formatIncidentMessage(incident Incident) slackMessage {
	headerText := s.incidentHeader(incident)
	headerText += fmt.Sprintf(" — %d services affected\nRoot cause: %s (deepest in chain)\nChain: %s",
		len(incident.Services), incident.RootService,
		strings.Join(incident.DepChain, " → "))
	if incident.Duration > 0 {
		headerText += fmt.Sprintf("\nDuration: %s", incident.Duration)
	}
	blocks := []slackBlock{
		{Type: "section", Text: &slackText{Type: "mrkdwn", Text: headerText}},
	}

	if incident.Diagnosis != "" {
		blocks = append(blocks, s.diagnosisBlocks(incident)...)
	}

	for _, alert := range incident.Alerts {
		alertHeader := fmt.Sprintf("*[%s]* %d× %s", slackEscape(alert.Service), alert.Count, alert.Level)
		blocks = append(blocks, slackBlock{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: alertHeader},
		})
		for _, p := range alert.Patterns {
			var sb strings.Builder
			badge := anomalyBadge(p)
			sb.WriteString(fmt.Sprintf("%s*[%dx %s]* `%s`\n", badge, p.Count, p.Level, slackEscape(p.Template)))
			for _, line := range p.SampleLines {
				sb.WriteString("• ")
				sb.WriteString(slackEscape(line))
				sb.WriteString("\n")
			}
			blocks = append(blocks, slackBlock{
				Type: "section",
				Text: &slackText{Type: "mrkdwn", Text: sb.String()},
			})
		}
	}
	return slackMessage{Blocks: blocks}
}

func (s *SlackNotifier) diagnosisBlocks(incident Incident) []slackBlock {
	var blocks []slackBlock
	diagText := fmt.Sprintf("\U0001F4CB *Diagnosis:*\n%s", slackEscape(incident.Diagnosis))
	blocks = append(blocks, slackBlock{
		Type: "section",
		Text: &slackText{Type: "mrkdwn", Text: diagText},
	})
	if len(incident.Suggestions) > 0 {
		var sb strings.Builder
		sb.WriteString("\U0001F4A1 *Suggested actions:*\n")
		for i, s := range incident.Suggestions {
			sb.WriteString(fmt.Sprintf("%d. %s\n", i+1, slackEscape(s)))
		}
		blocks = append(blocks, slackBlock{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: sb.String()},
		})
	}
	return blocks
}

func (s *SlackNotifier) incidentHeader(inc Incident) string {
	sev := inc.Severity
	if sev == "" {
		sev = "INCIDENT"
	}
	switch inc.EventType {
	case "resolved":
		return fmt.Sprintf("✅ *%s INCIDENT RESOLVED %s*", sev, inc.ID)
	case "updated":
		return fmt.Sprintf("🔄 *%s INCIDENT UPDATE %s*", sev, inc.ID)
	default:
		return fmt.Sprintf("🔴 *%s INCIDENT OPENED %s*", sev, inc.ID)
	}
}

// anomalyBadge returns a Slack emoji prefix for anomalous patterns.
// Returns an empty string for AnomalyNone (steady-state patterns).
func anomalyBadge(p PatternSummary) string {
	switch p.Anomaly {
	case AnomalyNewPattern:
		return ":new: "
	case AnomalySpike:
		return fmt.Sprintf(":chart_with_upward_trend: *+%.1f\u03c3* ", p.ZScore)
	case AnomalyRateJump:
		return fmt.Sprintf(":zap: *+%.1f\u03c3* ", p.ZScore)
	default:
		return ""
	}
}
