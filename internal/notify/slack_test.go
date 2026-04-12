package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSlackNotifier_Send(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type = %s, want application/json", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{
		Service: "myapp",
		Level:   "ERROR",
		Count:   42,
		Window:  5 * time.Minute,
		SampleLines: []string{
			"connection refused",
			"disk <full> & slow",
		},
		Timestamp: time.Now(),
	}
	inc := Incident{Alerts: []Alert{alert}, Services: []string{alert.Service}}

	err := sn.Send(context.Background(), inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify JSON is valid
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks, ok := msg["blocks"].([]interface{})
	if !ok {
		t.Fatal("expected blocks array")
	}
	if len(blocks) < 2 {
		t.Fatalf("expected at least 2 blocks, got %d", len(blocks))
	}

	// Verify HTML escaping in sample block
	sampleBlock := blocks[1].(map[string]interface{})
	text := sampleBlock["text"].(map[string]interface{})["text"].(string)
	if !contains(text, "&lt;full&gt;") {
		t.Errorf("expected HTML-escaped angle brackets in %q", text)
	}
	if !contains(text, "&amp;") {
		t.Errorf("expected HTML-escaped ampersand in %q", text)
	}
}

func TestSlackNotifier_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("webhook error"))
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{Service: "svc", Level: "ERROR", Count: 1, Window: time.Minute}
	err := sn.Send(context.Background(), Incident{Alerts: []Alert{alert}, Services: []string{alert.Service}})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestSlackNotifier_Name(t *testing.T) {
	sn := NewSlackNotifier("http://example.com")
	if sn.Name() != "slack" {
		t.Errorf("name = %s, want slack", sn.Name())
	}
}

func TestLevelEmoji(t *testing.T) {
	tests := []struct {
		level string
		emoji string
	}{
		{"FATAL", "\U0001F480"},
		{"ERROR", "\U0001F534"},
		{"WARN", "\U0001F7E1"},
		{"UNKNOWN", "\u2139\uFE0F"},
	}
	for _, tc := range tests {
		got := levelEmoji(tc.level)
		if got != tc.emoji {
			t.Errorf("levelEmoji(%q) = %q, want %q", tc.level, got, tc.emoji)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsImpl(s, substr))
}

func containsImpl(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestSlackNotifier_PatternBlocks(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{
		Service: "myapp",
		Level:   "ERROR",
		Count:   5,
		Window:  1 * time.Minute,
		Patterns: []PatternSummary{
			{
				Template:    "connection timeout to <*>",
				Count:       3,
				Level:       "ERROR",
				SampleLines: []string{"connection timeout to host1"},
			},
			{
				Template:    "disk write error",
				Count:       2,
				Level:       "WARN",
				SampleLines: []string{"disk write error"},
			},
		},
		Timestamp: time.Now(),
	}

	inc := Incident{Alerts: []Alert{alert}, Services: []string{alert.Service}}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks, ok := msg["blocks"].([]interface{})
	if !ok {
		t.Fatal("expected blocks array")
	}
	// Header block + 2 pattern blocks.
	if len(blocks) < 3 {
		t.Fatalf("expected at least 3 blocks (header + 2 patterns), got %d", len(blocks))
	}

	// Check that pattern template appears in one of the blocks.
	found := false
	for _, b := range blocks[1:] {
		block := b.(map[string]interface{})
		text := block["text"].(map[string]interface{})["text"].(string)
		if contains(text, "connection timeout to") {
			found = true
		}
	}
	if !found {
		t.Errorf("pattern template not found in slack blocks")
	}
}

func TestSlackNotifier_FallsBackToSamples(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	// Alert with no Patterns → should fall back to SampleLines.
	alert := Alert{
		Service:     "myapp",
		Level:       "ERROR",
		Count:       2,
		Window:      1 * time.Minute,
		SampleLines: []string{"error line 1", "error line 2"},
		Timestamp:   time.Now(),
	}

	if err := sn.Send(context.Background(), Incident{Alerts: []Alert{alert}, Services: []string{alert.Service}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks, _ := msg["blocks"].([]interface{})
	if len(blocks) < 2 {
		t.Fatalf("expected at least 2 blocks (header + samples), got %d", len(blocks))
	}
	text := blocks[1].(map[string]interface{})["text"].(map[string]interface{})["text"].(string)
	if !contains(text, "Samples") {
		t.Errorf("expected 'Samples' heading in fallback block, got: %s", text)
	}
}

// --- Anomaly rendering tests ---

func TestSlackNotifier_SpikePatternHasEmoji(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{
		Service: "svc",
		Level:   "ERROR",
		Count:   200,
		Window:  1 * time.Minute,
		Patterns: []PatternSummary{
			{Template: "timeout <*>", Count: 200, Level: "ERROR", Anomaly: AnomalySpike, ZScore: 4.2},
		},
	}
	inc := Incident{Alerts: []Alert{alert}, Services: []string{alert.Service}}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	found := false
	for _, b := range blocks[1:] {
		block := b.(map[string]interface{})
		text := block["text"].(map[string]interface{})["text"].(string)
		if contains(text, ":chart_with_upward_trend:") {
			found = true
		}
	}
	if !found {
		t.Error("expected spike emoji in slack blocks")
	}
}

func TestSlackNotifier_NewPatternHasEmoji(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{
		Service: "svc",
		Level:   "ERROR",
		Count:   1,
		Window:  1 * time.Minute,
		Patterns: []PatternSummary{
			{Template: "new error", Count: 1, Level: "ERROR", Anomaly: AnomalyNewPattern},
		},
	}
	inc := Incident{Alerts: []Alert{alert}, Services: []string{alert.Service}}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	found := false
	for _, b := range blocks[1:] {
		block := b.(map[string]interface{})
		text := block["text"].(map[string]interface{})["text"].(string)
		if contains(text, ":new:") {
			found = true
		}
	}
	if !found {
		t.Error("expected new-pattern emoji in slack blocks")
	}
}

func TestSlackNotifier_NoAnomalyPatternHasNoEmoji(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{
		Service: "svc",
		Level:   "ERROR",
		Count:   5,
		Window:  1 * time.Minute,
		Patterns: []PatternSummary{
			{Template: "steady error", Count: 5, Level: "ERROR", Anomaly: AnomalyNone},
		},
	}
	inc := Incident{Alerts: []Alert{alert}, Services: []string{alert.Service}}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	for _, b := range blocks[1:] {
		block := b.(map[string]interface{})
		text := block["text"].(map[string]interface{})["text"].(string)
		if contains(text, ":chart_with_upward_trend:") || contains(text, ":new:") || contains(text, ":zap:") {
			t.Errorf("unexpected anomaly emoji in steady-state block: %s", text)
		}
	}
}

// --- Incident rendering tests ---

func TestSlackNotifier_MultiServiceIncidentBlocks(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	inc := Incident{
		ID:          "abc123",
		Services:    []string{"svc-A", "svc-B"},
		RootService: "svc-B",
		DepChain:    []string{"svc-B", "svc-A"},
		Alerts: []Alert{
			{Service: "svc-A", Level: "ERROR", Count: 10, Window: time.Minute, Patterns: []PatternSummary{{Template: "timeout", Count: 10, Level: "ERROR"}}},
			{Service: "svc-B", Level: "ERROR", Count: 20, Window: time.Minute, Patterns: []PatternSummary{{Template: "conn refused", Count: 20, Level: "ERROR"}}},
		},
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	// Header + at least 2 service sections.
	if len(blocks) < 3 {
		t.Fatalf("expected at least 3 blocks, got %d", len(blocks))
	}
	headerText := blocks[0].(map[string]interface{})["text"].(map[string]interface{})["text"].(string)
	if !contains(headerText, "INCIDENT") {
		t.Errorf("expected INCIDENT in header, got: %s", headerText)
	}
	if !contains(headerText, "svc-B") {
		t.Errorf("expected root cause svc-B in header, got: %s", headerText)
	}
}

func TestSlackNotifier_SingleAlertIncidentBackwardCompat(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{Service: "svc-A", Level: "ERROR", Count: 5, Window: time.Minute, SampleLines: []string{"error line"}}
	inc := Incident{Alerts: []Alert{alert}, Services: []string{"svc-A"}}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	headerText := blocks[0].(map[string]interface{})["text"].(map[string]interface{})["text"].(string)
	// Should NOT contain INCIDENT header (backward compat).
	if contains(headerText, "INCIDENT") {
		t.Errorf("single-alert incident should not have INCIDENT header, got: %s", headerText)
	}
}

func TestSlackNotifier_IncidentDepChainInHeader(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	inc := Incident{
		ID:          "xyz789",
		Services:    []string{"A", "B", "C"},
		RootService: "C",
		DepChain:    []string{"C", "B", "A"},
		Alerts: []Alert{
			{Service: "C", Level: "ERROR", Count: 1, Window: time.Minute},
			{Service: "B", Level: "ERROR", Count: 1, Window: time.Minute},
			{Service: "A", Level: "ERROR", Count: 1, Window: time.Minute},
		},
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	headerText := blocks[0].(map[string]interface{})["text"].(map[string]interface{})["text"].(string)
	if !contains(headerText, "C") || !contains(headerText, "B") || !contains(headerText, "A") {
		t.Errorf("expected dep chain in header, got: %s", headerText)
	}
}

// --- Diagnosis rendering tests (Phase 5) ---

func TestSlackNotifier_IncidentWithDiagnosisBlocks(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	inc := Incident{
		ID:          "diag-slack",
		Services:    []string{"svc-A", "svc-B"},
		RootService: "svc-B",
		DepChain:    []string{"svc-B", "svc-A"},
		Alerts: []Alert{
			{Service: "svc-A", Level: "ERROR", Count: 10, Window: time.Minute, Patterns: []PatternSummary{{Template: "timeout", Count: 10, Level: "ERROR"}}},
		},
		Severity:    "P1",
		Diagnosis:   "root cause explanation",
		Suggestions: []string{"action 1", "action 2"},
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	// Header + diagnosis + suggestions + alert section = at least 4 blocks.
	if len(blocks) < 4 {
		t.Fatalf("expected at least 4 blocks, got %d", len(blocks))
	}
	// Check header contains severity.
	headerText := blocks[0].(map[string]interface{})["text"].(map[string]interface{})["text"].(string)
	if !contains(headerText, "P1") {
		t.Errorf("expected P1 in header, got: %s", headerText)
	}
	// Check diagnosis block exists.
	foundDiag := false
	foundSug := false
	for _, b := range blocks[1:] {
		block := b.(map[string]interface{})
		text := block["text"].(map[string]interface{})["text"].(string)
		if contains(text, "root cause explanation") {
			foundDiag = true
		}
		if contains(text, "action 1") {
			foundSug = true
		}
	}
	if !foundDiag {
		t.Error("expected diagnosis block with 'root cause explanation'")
	}
	if !foundSug {
		t.Error("expected suggestions block with 'action 1'")
	}
}

func TestSlackNotifier_IncidentDiagnosisEmptyBackwardCompat(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	inc := Incident{
		ID:          "no-diag",
		Services:    []string{"svc-A", "svc-B"},
		RootService: "svc-B",
		DepChain:    []string{"svc-B", "svc-A"},
		Alerts: []Alert{
			{Service: "svc-A", Level: "ERROR", Count: 10, Window: time.Minute, Patterns: []PatternSummary{{Template: "timeout", Count: 10, Level: "ERROR"}}},
		},
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	for _, b := range blocks {
		block := b.(map[string]interface{})
		text := block["text"].(map[string]interface{})["text"].(string)
		if contains(text, "Diagnosis") || contains(text, "Suggested actions") {
			t.Errorf("empty diagnosis should not have diagnosis/suggestion blocks, got: %s", text)
		}
	}
}

func TestSlackNotifier_IncidentSuggestionsAsNumberedList(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	inc := Incident{
		ID:          "sug-test",
		Services:    []string{"svc-A", "svc-B"},
		RootService: "svc-B",
		DepChain:    []string{"svc-B", "svc-A"},
		Alerts: []Alert{
			{Service: "svc-A", Level: "ERROR", Count: 10, Window: time.Minute},
		},
		Severity:    "P2",
		Diagnosis:   "something wrong",
		Suggestions: []string{"first thing", "second thing", "third thing"},
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	foundNumbered := false
	for _, b := range blocks {
		block := b.(map[string]interface{})
		text := block["text"].(map[string]interface{})["text"].(string)
		if contains(text, "1.") && contains(text, "2.") && contains(text, "3.") {
			foundNumbered = true
		}
	}
	if !foundNumbered {
		t.Error("expected numbered suggestions (1., 2., 3.)")
	}
}

func TestSlackNotifier_SingleAlertWithDiagnosis(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{Service: "svc-X", Level: "ERROR", Count: 5, Window: time.Minute, SampleLines: []string{"error line"}}
	inc := Incident{
		Alerts:      []Alert{alert},
		Services:    []string{"svc-X"},
		Diagnosis:   "service X is down",
		Severity:    "P3",
		Suggestions: []string{"restart service X"},
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks := msg["blocks"].([]interface{})
	// Should NOT have INCIDENT header (single alert).
	headerText := blocks[0].(map[string]interface{})["text"].(map[string]interface{})["text"].(string)
	if contains(headerText, "INCIDENT") {
		t.Errorf("single-alert should not have INCIDENT header, got: %s", headerText)
	}
	// Should have diagnosis block.
	foundDiag := false
	for _, b := range blocks {
		block := b.(map[string]interface{})
		text := block["text"].(map[string]interface{})["text"].(string)
		if contains(text, "service X is down") {
			foundDiag = true
		}
	}
	if !foundDiag {
		t.Error("expected diagnosis block in single-alert with diagnosis")
	}
}

// --- Event type header tests ---

func TestSlackNotifier_IncidentHeader_Opened(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	inc := Incident{
		ID:          "hdr-1",
		Services:    []string{"svc-a", "svc-b"},
		RootService: "svc-b",
		DepChain:    []string{"svc-b", "svc-a"},
		Alerts:      []Alert{{Service: "svc-b", Level: "ERROR", Count: 5, Window: time.Minute}},
		Severity:    "P2",
		EventType:   "opened",
		Status:      StatusOpen,
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	body := string(receivedBody)
	if !contains(body, "OPENED") {
		t.Error("expected OPENED in header")
	}
	if !contains(body, "P2") {
		t.Error("expected P2 in header")
	}
}

func TestSlackNotifier_IncidentHeader_Resolved(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	inc := Incident{
		ID:          "hdr-2",
		Services:    []string{"svc-a"},
		RootService: "svc-a",
		DepChain:    []string{"svc-a"},
		Alerts:      []Alert{{Service: "svc-a", Level: "ERROR", Count: 1, Window: time.Minute}},
		Severity:    "P1",
		EventType:   "resolved",
		Status:      StatusResolved,
		Duration:    3 * time.Minute,
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	body := string(receivedBody)
	if !contains(body, "RESOLVED") {
		t.Error("expected RESOLVED in header")
	}
	if !contains(body, "Duration") {
		t.Error("expected Duration in body")
	}
}

func TestSlackNotifier_IncidentHeader_Updated(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	inc := Incident{
		ID:          "hdr-3",
		Services:    []string{"svc-a", "svc-b"},
		RootService: "svc-b",
		DepChain:    []string{"svc-b", "svc-a"},
		Alerts:      []Alert{{Service: "svc-b", Level: "ERROR", Count: 10, Window: time.Minute}},
		Severity:    "P1",
		EventType:   "updated",
		Status:      StatusOngoing,
	}
	if err := sn.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	body := string(receivedBody)
	if !contains(body, "UPDATE") {
		t.Error("expected UPDATE in header")
	}
}
