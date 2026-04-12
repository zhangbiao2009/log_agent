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

func (s *SlackNotifier) Send(ctx context.Context, alert Alert) error {
	msg := s.formatMessage(alert)
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

func (s *SlackNotifier) formatMessage(alert Alert) slackMessage {
	emoji := levelEmoji(alert.Level)
	header := fmt.Sprintf("%s *%s* — %s (%d errors in last %s)",
		emoji, alert.Level, alert.Service, alert.Count, alert.Window)
	blocks := []slackBlock{
		{Type: "section", Text: &slackText{Type: "mrkdwn", Text: header}},
	}
	if len(alert.Patterns) > 0 {
		for _, p := range alert.Patterns {
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("*[%dx %s]* `%s`\n", p.Count, p.Level, slackEscape(p.Template)))
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
