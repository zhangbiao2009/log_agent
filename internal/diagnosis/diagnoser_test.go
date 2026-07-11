package diagnosis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/core"
)

// MockLLM implements LLMClient for testing.
type MockLLM struct {
	Response       string
	Err            error
	CapturedPrompt string
	CallCount      int
	Block          chan struct{} // if non-nil, Complete blocks until closed or ctx cancelled
}

func (m *MockLLM) Complete(ctx context.Context, prompt string) (string, error) {
	m.CapturedPrompt = prompt
	m.CallCount++
	if m.Block != nil {
		select {
		case <-m.Block:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return m.Response, m.Err
}

func makeIncident(services ...string) core.Incident {
	var alerts []core.Alert
	for _, svc := range services {
		alerts = append(alerts, core.Alert{
			Service:   svc,
			Level:     "ERROR",
			Count:     10,
			Window:    1 * time.Minute,
			Timestamp: time.Date(2026, 4, 12, 14, 30, 0, 0, time.UTC),
			Patterns: []core.PatternSummary{
				{
					Template:    "connection refused to <*>:<*>",
					Count:       10,
					Level:       "ERROR",
					SampleLines: []string{"connection refused to host:443"},
					Anomaly:     core.AnomalySpike,
					ZScore:      5.2,
				},
			},
		})
	}
	inc := core.Incident{
		ID:       "test123",
		Services: services,
		Alerts:   alerts,
		OpenedAt: time.Date(2026, 4, 12, 14, 30, 0, 0, time.UTC),
		Window:   2 * time.Minute,
	}
	if len(services) > 1 {
		inc.RootService = services[len(services)-1]
		inc.DepChain = services
	}
	return inc
}

func collectIncidents(out <-chan core.Incident, n int, timeout time.Duration) []core.Incident {
	var results []core.Incident
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for i := 0; i < n; i++ {
		select {
		case inc, ok := <-out:
			if !ok {
				return results
			}
			results = append(results, inc)
		case <-timer.C:
			return results
		}
	}
	return results
}

// --- Pipeline Mechanics ---

func TestDiagnoser_ClosesOutputWhenInputCloses(t *testing.T) {
	mock := &MockLLM{Response: "SEVERITY: P3\nDIAGNOSIS: ok"}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	in := make(chan core.Incident)
	close(in)
	out := d.Run(context.Background(), in)
	// Output should close.
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case _, ok := <-out:
		if ok {
			t.Error("expected output channel to be closed")
		}
	case <-timer.C:
		t.Fatal("output channel did not close within timeout")
	}
}

func TestDiagnoser_ClosesOutputOnContextCancel(t *testing.T) {
	block := make(chan struct{})
	mock := &MockLLM{Response: "SEVERITY: P1\nDIAGNOSIS: test", Block: block}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan core.Incident, 1)
	in <- makeIncident("svc-a")
	out := d.Run(ctx, in)
	// Cancel while LLM is blocked.
	time.Sleep(50 * time.Millisecond)
	cancel()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	// Drain any output and wait for channel close.
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return // success
			}
		case <-timer.C:
			t.Fatal("output channel did not close within 1s after cancel")
		}
	}
}

func TestDiagnoser_PassesThroughAllIncidents(t *testing.T) {
	mock := &MockLLM{Response: "SEVERITY: P2\nDIAGNOSIS: ok\nSUGGESTIONS:\n- fix"}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	in := make(chan core.Incident, 3)
	in <- makeIncident("svc-a")
	in <- makeIncident("svc-b")
	in <- makeIncident("svc-c")
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 3, 5*time.Second)
	if len(results) != 3 {
		t.Fatalf("got %d incidents, want 3", len(results))
	}
	if mock.CallCount != 3 {
		t.Errorf("CallCount = %d, want 3", mock.CallCount)
	}
}

// --- Enrichment ---

func TestDiagnoser_SetsDiagnosisField(t *testing.T) {
	mock := &MockLLM{Response: "SEVERITY: P1\nDIAGNOSIS: root cause explanation\nSUGGESTIONS:\n- fix"}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	in := make(chan core.Incident, 1)
	in <- makeIncident("svc-a")
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)
	if len(results) != 1 {
		t.Fatal("expected 1 incident")
	}
	if results[0].Diagnosis != "root cause explanation" {
		t.Errorf("Diagnosis = %q, want 'root cause explanation'", results[0].Diagnosis)
	}
}

func TestDiagnoser_SetsSeverityField(t *testing.T) {
	mock := &MockLLM{Response: "SEVERITY: P1\nDIAGNOSIS: test"}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	in := make(chan core.Incident, 1)
	in <- makeIncident("svc-a")
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)
	if results[0].Severity != "P1" {
		t.Errorf("Severity = %q, want P1", results[0].Severity)
	}
}

func TestDiagnoser_SetsSuggestionsField(t *testing.T) {
	mock := &MockLLM{Response: "SEVERITY: P2\nDIAGNOSIS: test\nSUGGESTIONS:\n- action 1\n- action 2\n- action 3"}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	in := make(chan core.Incident, 1)
	in <- makeIncident("svc-a")
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)
	if len(results[0].Suggestions) != 3 {
		t.Errorf("len(Suggestions) = %d, want 3", len(results[0].Suggestions))
	}
}

// --- Failure / Fallback ---

func TestDiagnoser_LLMErrorFallbackDiagnosis(t *testing.T) {
	mock := &MockLLM{Err: errors.New("connection refused")}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	in := make(chan core.Incident, 1)
	in <- makeIncident("svc-a")
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)
	if len(results) != 1 {
		t.Fatal("expected 1 incident")
	}
	if !strings.Contains(results[0].Diagnosis, "LLM diagnosis unavailable") {
		t.Errorf("Diagnosis = %q, want 'LLM diagnosis unavailable...'", results[0].Diagnosis)
	}
	if results[0].Severity == "" {
		t.Error("Severity should be set by heuristic")
	}
	if results[0].Suggestions != nil {
		t.Errorf("Suggestions = %v, want nil", results[0].Suggestions)
	}
}

func TestDiagnoser_LLMErrorPreservesExistingFields(t *testing.T) {
	mock := &MockLLM{Err: errors.New("api error")}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	inc := makeIncident("svc-a", "svc-b")
	in := make(chan core.Incident, 1)
	in <- inc
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)
	r := results[0]
	if len(r.Services) != 2 || r.Services[0] != "svc-a" || r.Services[1] != "svc-b" {
		t.Errorf("Services = %v, want [svc-a svc-b]", r.Services)
	}
	if r.RootService != "svc-b" {
		t.Errorf("RootService = %q, want svc-b", r.RootService)
	}
	if len(r.Alerts) != 2 {
		t.Errorf("len(Alerts) = %d, want 2", len(r.Alerts))
	}
	if r.ID != "test123" {
		t.Errorf("ID = %q, want test123", r.ID)
	}
}

func TestDiagnoser_HeuristicSeverityP1(t *testing.T) {
	mock := &MockLLM{Err: errors.New("fail")}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	inc := makeIncident("svc-a", "svc-b", "svc-c")
	in := make(chan core.Incident, 1)
	in <- inc
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)
	if results[0].Severity != "P1" {
		t.Errorf("Severity = %q, want P1 (>=3 services)", results[0].Severity)
	}
}

func TestDiagnoser_HeuristicSeverityP2(t *testing.T) {
	mock := &MockLLM{Err: errors.New("fail")}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	inc := makeIncident("svc-a", "svc-b")
	in := make(chan core.Incident, 1)
	in <- inc
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)
	if results[0].Severity != "P2" {
		t.Errorf("Severity = %q, want P2 (2 services)", results[0].Severity)
	}
}

func TestDiagnoser_HeuristicSeverityP3(t *testing.T) {
	mock := &MockLLM{Err: errors.New("fail")}
	d := NewDiagnoser(DiagnoserConfig{}, mock)
	inc := makeIncident("svc-a")
	// Override to non-FATAL, no spikes.
	inc.Alerts[0].Level = "ERROR"
	inc.Alerts[0].Patterns = nil
	in := make(chan core.Incident, 1)
	in <- inc
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)
	if results[0].Severity != "P3" {
		t.Errorf("Severity = %q, want P3 (single service, non-critical)", results[0].Severity)
	}
}

// --- E2E Integration ---

func TestDiagnoser_EndToEnd_MultiServiceIncident(t *testing.T) {
	cannedResponse := `SEVERITY: P1
DIAGNOSIS: bank-gateway v2.3.1 deployed at 14:30 is refusing connections
on port 443. payment-service cannot reach bank-gateway, causing timeouts
in order-service.
SUGGESTIONS:
- Rollback bank-gateway to v2.3.0
- Check bank-gateway deployment logs for startup errors
- Monitor error rates after rollback`

	mock := &MockLLM{Response: cannedResponse}
	d := NewDiagnoser(DiagnoserConfig{}, mock)

	inc := core.Incident{
		ID:          "e2e-test",
		Services:    []string{"bank-gw", "payment-svc", "order-svc"},
		RootService: "bank-gw",
		DepChain:    []string{"bank-gw", "payment-svc", "order-svc"},
		Alerts: []core.Alert{
			{
				Service: "bank-gw", Level: "ERROR", Count: 50, Window: time.Minute,
				Timestamp: time.Date(2026, 4, 12, 14, 30, 0, 0, time.UTC),
				Patterns: []core.PatternSummary{
					{Template: "connection refused <*>:<*>", Count: 50, Level: "ERROR", Anomaly: core.AnomalySpike, ZScore: 8.1, SampleLines: []string{"connection refused bank-gw:443"}},
				},
			},
			{
				Service: "payment-svc", Level: "ERROR", Count: 30, Window: time.Minute,
				Timestamp: time.Date(2026, 4, 12, 14, 30, 0, 0, time.UTC),
				Patterns: []core.PatternSummary{
					{Template: "timeout calling bank-gw", Count: 30, Level: "ERROR", Anomaly: core.AnomalySpike, ZScore: 6.3, SampleLines: []string{"timeout calling bank-gw"}},
				},
			},
			{
				Service: "order-svc", Level: "ERROR", Count: 20, Window: time.Minute,
				Timestamp: time.Date(2026, 4, 12, 14, 30, 0, 0, time.UTC),
				Patterns: []core.PatternSummary{
					{Template: "upstream error from payment-svc", Count: 20, Level: "ERROR", Anomaly: core.AnomalySpike, ZScore: 5.0, SampleLines: []string{"upstream error from payment-svc"}},
				},
			},
		},
		OpenedAt: time.Date(2026, 4, 12, 14, 30, 0, 0, time.UTC),
		Window:   2 * time.Minute,
	}

	in := make(chan core.Incident, 1)
	in <- inc
	close(in)
	out := d.Run(context.Background(), in)
	results := collectIncidents(out, 1, 5*time.Second)

	if len(results) != 1 {
		t.Fatalf("got %d incidents, want 1", len(results))
	}
	r := results[0]

	// Enrichment checks.
	if r.Severity != "P1" {
		t.Errorf("Severity = %q, want P1", r.Severity)
	}
	if !strings.Contains(r.Diagnosis, "bank-gateway") {
		t.Errorf("Diagnosis missing bank-gateway: %q", r.Diagnosis)
	}
	if !strings.Contains(r.Diagnosis, "refusing connections") {
		t.Errorf("Diagnosis missing 'refusing connections': %q", r.Diagnosis)
	}
	if len(r.Suggestions) != 3 {
		t.Fatalf("len(Suggestions) = %d, want 3", len(r.Suggestions))
	}
	if !strings.Contains(r.Suggestions[0], "Rollback") {
		t.Errorf("Suggestions[0] = %q, want Rollback...", r.Suggestions[0])
	}

	// Original fields preserved.
	if r.ID != "e2e-test" {
		t.Errorf("ID = %q, want e2e-test", r.ID)
	}
	if len(r.Services) != 3 {
		t.Errorf("len(Services) = %d, want 3", len(r.Services))
	}
	if r.RootService != "bank-gw" {
		t.Errorf("RootService = %q, want bank-gw", r.RootService)
	}
	if len(r.DepChain) != 3 {
		t.Errorf("len(DepChain) = %d, want 3", len(r.DepChain))
	}
	if len(r.Alerts) != 3 {
		t.Errorf("len(Alerts) = %d, want 3", len(r.Alerts))
	}

	// MockLLM checks.
	if mock.CallCount != 1 {
		t.Errorf("CallCount = %d, want 1", mock.CallCount)
	}
	for _, svc := range []string{"bank-gw", "payment-svc", "order-svc"} {
		if !strings.Contains(mock.CapturedPrompt, svc) {
			t.Errorf("CapturedPrompt missing service %q", svc)
		}
	}
}
