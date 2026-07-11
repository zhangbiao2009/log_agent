package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/zhangbiao2009/log_agent/internal/core"
	"io"
	"net/http"
	"strings"
)

// TeamsConfig holds Microsoft Teams webhook settings.
type TeamsConfig struct {
	WebhookURL string
}

// TeamsNotifier sends incident notifications to Microsoft Teams
// via an incoming webhook using Adaptive Card format.
type TeamsNotifier struct {
	cfg    TeamsConfig
	client *http.Client
}

// NewTeamsNotifier creates a TeamsNotifier.
func NewTeamsNotifier(cfg TeamsConfig) *TeamsNotifier {
	return &TeamsNotifier{
		cfg:    cfg,
		client: &http.Client{Timeout: 10 * 1e9},
	}
}

func (t *TeamsNotifier) Name() string { return "teams" }

func (t *TeamsNotifier) Send(ctx context.Context, incident core.Incident) error {
	if t.cfg.WebhookURL == "" {
		return fmt.Errorf("teams: empty webhook URL")
	}

	card := t.buildCard(incident)
	body, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("teams: marshal card: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.cfg.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("teams: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("teams: webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("teams: webhook returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// Adaptive Card types

type adaptiveCardEnvelope struct {
	Type        string         `json:"type"`
	Attachments []acAttachment `json:"attachments"`
}

type acAttachment struct {
	ContentType string       `json:"contentType"`
	ContentURL  *string      `json:"contentUrl"`
	Content     adaptiveCard `json:"content"`
}

type adaptiveCard struct {
	Schema  string          `json:"$schema"`
	Type    string          `json:"type"`
	Version string          `json:"version"`
	Body    []acElement     `json:"body"`
	MSTeams *acMSTeamsProps `json:"msteams,omitempty"`
}

type acMSTeamsProps struct {
	Width string `json:"width,omitempty"`
}

type acElement struct {
	Type      string      `json:"type"`
	Text      string      `json:"text,omitempty"`
	Size      string      `json:"size,omitempty"`
	Weight    string      `json:"weight,omitempty"`
	Color     string      `json:"color,omitempty"`
	Wrap      *bool       `json:"wrap,omitempty"`
	Separator *bool       `json:"separator,omitempty"`
	Facts     []acFact    `json:"facts,omitempty"`
	Items     []acElement `json:"items,omitempty"`
}

type acFact struct {
	Title string `json:"title"`
	Value string `json:"value"`
}

func boolPtr(v bool) *bool { return &v }

func (t *TeamsNotifier) buildCard(inc core.Incident) adaptiveCardEnvelope {
	color := teamsSeverityColor(inc.Severity)
	title := t.buildTitle(inc)

	body := []acElement{
		{
			Type:   "TextBlock",
			Text:   title,
			Size:   "Large",
			Weight: "Bolder",
			Color:  color,
			Wrap:   boolPtr(true),
		},
	}

	// Facts section
	facts := []acFact{}
	if inc.Severity != "" {
		facts = append(facts, acFact{Title: "Severity", Value: inc.Severity})
	}
	if inc.RootService != "" {
		facts = append(facts, acFact{Title: "Root Cause", Value: inc.RootService})
	}
	if len(inc.Services) > 0 {
		facts = append(facts, acFact{Title: "Affected Services", Value: strings.Join(inc.Services, ", ")})
	}
	if len(inc.DepChain) > 0 {
		facts = append(facts, acFact{Title: "Chain", Value: strings.Join(inc.DepChain, " → ")})
	}
	if inc.Duration > 0 {
		facts = append(facts, acFact{Title: "Duration", Value: inc.Duration.String()})
	}
	if len(facts) > 0 {
		body = append(body, acElement{
			Type:  "FactSet",
			Facts: facts,
		})
	}

	// Diagnosis
	if inc.Diagnosis != "" {
		body = append(body, acElement{
			Type:      "TextBlock",
			Text:      "**Diagnosis**",
			Weight:    "Bolder",
			Separator: boolPtr(true),
			Wrap:      boolPtr(true),
		})
		body = append(body, acElement{
			Type: "TextBlock",
			Text: inc.Diagnosis,
			Wrap: boolPtr(true),
		})
	}

	// Suggestions
	if len(inc.Suggestions) > 0 {
		body = append(body, acElement{
			Type:      "TextBlock",
			Text:      "**Suggested Actions**",
			Weight:    "Bolder",
			Separator: boolPtr(true),
			Wrap:      boolPtr(true),
		})
		var sb strings.Builder
		for i, s := range inc.Suggestions {
			sb.WriteString(fmt.Sprintf("%d. %s\n", i+1, s))
		}
		body = append(body, acElement{
			Type: "TextBlock",
			Text: sb.String(),
			Wrap: boolPtr(true),
		})
	}

	return adaptiveCardEnvelope{
		Type: "message",
		Attachments: []acAttachment{
			{
				ContentType: "application/vnd.microsoft.card.adaptive",
				ContentURL:  nil,
				Content: adaptiveCard{
					Schema:  "http://adaptivecards.io/schemas/adaptive-card.json",
					Type:    "AdaptiveCard",
					Version: "1.4",
					Body:    body,
					MSTeams: &acMSTeamsProps{Width: "Full"},
				},
			},
		},
	}
}

func (t *TeamsNotifier) buildTitle(inc core.Incident) string {
	service := inc.RootService
	if service == "" && len(inc.Services) > 0 {
		service = inc.Services[0]
	}

	sev := inc.Severity
	if sev == "" {
		sev = "INFO"
	}

	switch inc.EventType {
	case "resolved":
		return fmt.Sprintf("✅ %s INCIDENT RESOLVED — %s", sev, service)
	case "updated":
		return fmt.Sprintf("🔄 %s INCIDENT UPDATE — %s", sev, service)
	default:
		return fmt.Sprintf("🔴 %s INCIDENT OPENED — %s", sev, service)
	}
}

func teamsSeverityColor(sev string) string {
	switch sev {
	case "P1":
		return "Attention"
	case "P2":
		return "Warning"
	case "P3":
		return "Accent"
	default:
		return "Default"
	}
}
