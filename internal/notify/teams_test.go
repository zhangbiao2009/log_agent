package notify

import (
	"context"
	"encoding/json"
	"github.com/zhangbiao2009/log_agent/internal/core"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func teamsTestIncident(eventType string) core.Incident {
	inc := core.Incident{
		ID:          "teams-123",
		Services:    []string{"order-service", "payment-service", "bank-gateway"},
		RootService: "bank-gateway",
		DepChain:    []string{"bank-gateway", "payment-service", "order-service"},
		Alerts: []core.Alert{
			{Service: "bank-gateway", Level: "ERROR", Count: 200, Window: time.Minute},
		},
		OpenedAt:    time.Now(),
		Window:      2 * time.Minute,
		Diagnosis:   "bank-gateway stopped responding",
		Severity:    "P1",
		Suggestions: []string{"Rollback bank-gateway", "Check DB connections"},
		Status:      core.StatusOpen,
		EventType:   eventType,
	}
	if eventType == "resolved" {
		inc.Status = core.StatusResolved
		inc.Duration = 5 * time.Minute
	}
	if eventType == "updated" {
		inc.Status = core.StatusOngoing
	}
	return inc
}

func TestTeams_Name(t *testing.T) {
	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: "http://example.com"})
	if tn.Name() != "teams" {
		t.Errorf("Name() = %q, want %q", tn.Name(), "teams")
	}
}

func TestTeams_EmptyWebhook(t *testing.T) {
	tn := NewTeamsNotifier(TeamsConfig{})
	err := tn.Send(context.Background(), teamsTestIncident("opened"))
	if err == nil {
		t.Fatal("expected error for empty webhook URL")
	}
}

func TestTeams_OpenedIncident_AdaptiveCard(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type = %s, want application/json", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	inc := teamsTestIncident("opened")
	if err := tn.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	var envelope adaptiveCardEnvelope
	if err := json.Unmarshal(receivedBody, &envelope); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if envelope.Type != "message" {
		t.Errorf("type = %q, want message", envelope.Type)
	}
	if len(envelope.Attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(envelope.Attachments))
	}
	card := envelope.Attachments[0].Content
	if card.Type != "AdaptiveCard" {
		t.Errorf("card type = %q, want AdaptiveCard", card.Type)
	}

	// Title block should contain OPENED
	if len(card.Body) < 1 {
		t.Fatal("card body is empty")
	}
	title := card.Body[0].Text
	if !strings.Contains(title, "OPENED") {
		t.Errorf("title = %q, expected OPENED", title)
	}
	if !strings.Contains(title, "P1") {
		t.Errorf("title = %q, expected P1", title)
	}
}

func TestTeams_ResolvedIncident_Title(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	inc := teamsTestIncident("resolved")
	if err := tn.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	var envelope adaptiveCardEnvelope
	json.Unmarshal(receivedBody, &envelope)
	title := envelope.Attachments[0].Content.Body[0].Text
	if !strings.Contains(title, "RESOLVED") {
		t.Errorf("title = %q, expected RESOLVED", title)
	}
}

func TestTeams_UpdatedIncident_Title(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	inc := teamsTestIncident("updated")
	if err := tn.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	var envelope adaptiveCardEnvelope
	json.Unmarshal(receivedBody, &envelope)
	title := envelope.Attachments[0].Content.Body[0].Text
	if !strings.Contains(title, "UPDATE") {
		t.Errorf("title = %q, expected UPDATE", title)
	}
}

func TestTeams_SeverityColors(t *testing.T) {
	tests := []struct {
		sev   string
		color string
	}{
		{"P1", "Attention"},
		{"P2", "Warning"},
		{"P3", "Accent"},
		{"", "Default"},
		{"P4", "Default"},
	}
	for _, tc := range tests {
		got := teamsSeverityColor(tc.sev)
		if got != tc.color {
			t.Errorf("teamsSeverityColor(%q) = %q, want %q", tc.sev, got, tc.color)
		}
	}
}

func TestTeams_FactSet_ContainsSeverityAndRoot(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	inc := teamsTestIncident("opened")
	tn.Send(context.Background(), inc)

	var envelope adaptiveCardEnvelope
	json.Unmarshal(receivedBody, &envelope)
	card := envelope.Attachments[0].Content

	// Find FactSet
	var factSet *acElement
	for i := range card.Body {
		if card.Body[i].Type == "FactSet" {
			factSet = &card.Body[i]
			break
		}
	}
	if factSet == nil {
		t.Fatal("FactSet not found in card body")
	}

	hasSev, hasRoot := false, false
	for _, f := range factSet.Facts {
		if f.Title == "Severity" && f.Value == "P1" {
			hasSev = true
		}
		if f.Title == "Root Cause" && f.Value == "bank-gateway" {
			hasRoot = true
		}
	}
	if !hasSev {
		t.Error("FactSet missing severity fact")
	}
	if !hasRoot {
		t.Error("FactSet missing root cause fact")
	}
}

func TestTeams_Duration_InFactSet(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	inc := teamsTestIncident("resolved")
	tn.Send(context.Background(), inc)

	var envelope adaptiveCardEnvelope
	json.Unmarshal(receivedBody, &envelope)
	card := envelope.Attachments[0].Content

	hasDuration := false
	for _, el := range card.Body {
		if el.Type == "FactSet" {
			for _, f := range el.Facts {
				if f.Title == "Duration" && strings.Contains(f.Value, "5m") {
					hasDuration = true
				}
			}
		}
	}
	if !hasDuration {
		t.Error("resolved incident should have Duration fact")
	}
}

func TestTeams_DiagnosisBlock(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	inc := teamsTestIncident("opened")
	tn.Send(context.Background(), inc)

	bodyStr := string(receivedBody)
	if !strings.Contains(bodyStr, "Diagnosis") {
		t.Error("card missing Diagnosis section")
	}
	if !strings.Contains(bodyStr, "bank-gateway stopped responding") {
		t.Error("card missing diagnosis text")
	}
}

func TestTeams_SuggestionsBlock(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	inc := teamsTestIncident("opened")
	tn.Send(context.Background(), inc)

	bodyStr := string(receivedBody)
	if !strings.Contains(bodyStr, "Rollback bank-gateway") {
		t.Error("card missing suggestion text")
	}
	if !strings.Contains(bodyStr, "Suggested Actions") {
		t.Error("card missing Suggested Actions header")
	}
}

func TestTeams_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("bad request"))
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	err := tn.Send(context.Background(), teamsTestIncident("opened"))
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error = %v, expected 500", err)
	}
}

func TestTeams_ContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tn := NewTeamsNotifier(TeamsConfig{WebhookURL: srv.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := tn.Send(ctx, teamsTestIncident("opened"))
	if err == nil {
		t.Fatal("expected error from context cancel")
	}
}
