package diagnosis

import (
	"strings"
	"testing"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/core"
)

func makeTestIncident(services ...string) core.Incident {
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

func TestBuildPrompt_ContainsSystemInstruction(t *testing.T) {
	inc := makeTestIncident("svc-a")
	prompt := BuildPrompt(inc)
	if !strings.HasPrefix(prompt, "You are an SRE assistant") {
		t.Errorf("prompt should start with system instruction, got: %s", prompt[:60])
	}
}

func TestBuildPrompt_ContainsServiceList(t *testing.T) {
	inc := makeTestIncident("order-svc", "payment-svc")
	prompt := BuildPrompt(inc)
	if !strings.Contains(prompt, "order-svc") || !strings.Contains(prompt, "payment-svc") {
		t.Error("prompt should contain all service names")
	}
	if !strings.Contains(prompt, "Affected services") {
		t.Error("prompt should contain 'Affected services' section")
	}
}

func TestBuildPrompt_ContainsDepChain(t *testing.T) {
	inc := makeTestIncident("bank-gw", "payment-svc", "order-svc")
	prompt := BuildPrompt(inc)
	// DepChain is set by makeTestIncident for multi-service incidents.
	if !strings.Contains(prompt, "bank-gw") || !strings.Contains(prompt, "order-svc") {
		t.Error("prompt should contain dep chain services")
	}
}

func TestBuildPrompt_ContainsPatternTemplates(t *testing.T) {
	inc := makeTestIncident("svc-a")
	inc.Alerts[0].Patterns = []core.PatternSummary{
		{Template: "connection refused to <*>:<*>", Count: 10, Level: "ERROR"},
		{Template: "timeout calling <*>", Count: 5, Level: "ERROR"},
	}
	prompt := BuildPrompt(inc)
	if !strings.Contains(prompt, "connection refused to <*>:<*>") {
		t.Error("prompt should contain first pattern template")
	}
	if !strings.Contains(prompt, "timeout calling <*>") {
		t.Error("prompt should contain second pattern template")
	}
}

func TestBuildPrompt_ContainsSampleLines(t *testing.T) {
	inc := makeTestIncident("svc-a")
	inc.Alerts[0].Patterns[0].SampleLines = []string{
		"connection refused to host1:443",
		"connection refused to host2:443",
		"connection refused to host3:443",
	}
	prompt := BuildPrompt(inc)
	for _, line := range inc.Alerts[0].Patterns[0].SampleLines {
		if !strings.Contains(prompt, line) {
			t.Errorf("prompt should contain sample line: %s", line)
		}
	}
}

func TestBuildPrompt_ContainsAnomalyTags(t *testing.T) {
	inc := makeTestIncident("svc-a")
	inc.Alerts[0].Patterns = []core.PatternSummary{
		{Template: "spike pattern", Count: 10, Level: "ERROR", Anomaly: core.AnomalySpike, ZScore: 5.2},
		{Template: "new pattern", Count: 1, Level: "WARN", Anomaly: core.AnomalyNewPattern},
	}
	prompt := BuildPrompt(inc)
	if !strings.Contains(prompt, "SPIKE") {
		t.Error("prompt should contain SPIKE tag")
	}
	if !strings.Contains(prompt, "NEW") {
		t.Error("prompt should contain NEW tag")
	}
}

func TestBuildPrompt_SingleAlertNoDepChain(t *testing.T) {
	inc := makeTestIncident("svc-a")
	// Single service: no RootService, no DepChain.
	inc.RootService = ""
	inc.DepChain = nil
	prompt := BuildPrompt(inc)
	if strings.Contains(prompt, "Dependency chain") {
		t.Error("single-alert prompt should not contain Dependency chain")
	}
	if strings.Contains(prompt, "Suspected root cause") {
		t.Error("single-alert prompt should not contain Suspected root cause")
	}
}

func TestBuildPrompt_EmptyPatterns(t *testing.T) {
	inc := makeTestIncident("svc-a")
	inc.Alerts[0].Patterns = nil
	// Should not panic.
	prompt := BuildPrompt(inc)
	if !strings.Contains(prompt, "svc-a") {
		t.Error("prompt should still contain service name")
	}
}

func TestBuildPrompt_TruncatesLargeIncidents(t *testing.T) {
	var services []string
	for i := 0; i < 12; i++ {
		services = append(services, "svc-"+string(rune('a'+i)))
	}
	inc := makeTestIncident(services...)
	prompt := BuildPrompt(inc)
	if !strings.Contains(prompt, "additional services omitted") {
		t.Error("prompt should note omitted services")
	}
	// Count service sections: [svc-X] patterns.
	count := strings.Count(prompt, "] \u2014")
	if count > maxPromptServices {
		t.Errorf("prompt should have at most %d service sections, got %d", maxPromptServices, count)
	}
}
