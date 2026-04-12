package notify

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type mockNotifier struct {
	sendFunc  func(ctx context.Context, inc Incident) error
	incidents []Incident
	mu        sync.Mutex
}

func (m *mockNotifier) Send(ctx context.Context, inc Incident) error {
	m.mu.Lock()
	m.incidents = append(m.incidents, inc)
	m.mu.Unlock()
	if m.sendFunc != nil {
		return m.sendFunc(ctx, inc)
	}
	return nil
}

func (m *mockNotifier) Name() string { return "mock" }

func (m *mockNotifier) getIncidents() []Incident {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]Incident, len(m.incidents))
	copy(cp, m.incidents)
	return cp
}

// wrapAlert creates a single-alert Incident for testing backward compat.
func wrapAlert(a Alert) Incident {
	return Incident{Alerts: []Alert{a}, Services: []string{a.Service}}
}

func TestDispatcher_AllSuccess(t *testing.T) {
	m1 := &mockNotifier{}
	m2 := &mockNotifier{}
	d := NewDispatcher(m1, m2)

	inc := wrapAlert(Alert{Service: "svc1", Level: "ERROR", Count: 5})
	err := d.Dispatch(context.Background(), inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(m1.getIncidents()) != 1 {
		t.Errorf("m1 got %d incidents, want 1", len(m1.getIncidents()))
	}
	if len(m2.getIncidents()) != 1 {
		t.Errorf("m2 got %d incidents, want 1", len(m2.getIncidents()))
	}
}

func TestDispatcher_PartialFailure(t *testing.T) {
	m1 := &mockNotifier{}
	m2 := &mockNotifier{
		sendFunc: func(ctx context.Context, inc Incident) error {
			return errors.New("slack down")
		},
	}
	d := NewDispatcher(m1, m2)

	inc := wrapAlert(Alert{Service: "svc1", Level: "ERROR", Count: 5})
	err := d.Dispatch(context.Background(), inc)
	if err == nil {
		t.Fatal("expected error from partial failure")
	}

	if len(m1.getIncidents()) != 1 {
		t.Errorf("m1 got %d incidents, want 1", len(m1.getIncidents()))
	}
}

func TestDispatcher_NoNotifiers(t *testing.T) {
	d := NewDispatcher()
	err := d.Dispatch(context.Background(), Incident{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDispatcher_AllFail(t *testing.T) {
	fail := func(ctx context.Context, inc Incident) error {
		return errors.New("fail")
	}
	m1 := &mockNotifier{sendFunc: fail}
	m2 := &mockNotifier{sendFunc: fail}
	d := NewDispatcher(m1, m2)

	err := d.Dispatch(context.Background(), wrapAlert(Alert{Service: "svc1"}))
	if err == nil {
		t.Fatal("expected error when all notifiers fail")
	}
}

// --- Severity routing tests ---

func TestRoutedDispatcher_P1MatchesAll(t *testing.T) {
	m := &mockNotifier{}
	d := NewRoutedDispatcher([]NotifierRoute{
		{Notifier: m, Severities: []string{"P1", "P2", "P3"}},
	})

	inc := Incident{Severity: "P1", Alerts: []Alert{{Service: "svc"}}, Services: []string{"svc"}}
	if err := d.Dispatch(context.Background(), inc); err != nil {
		t.Fatal(err)
	}
	if len(m.getIncidents()) != 1 {
		t.Errorf("expected 1 incident, got %d", len(m.getIncidents()))
	}
}

func TestRoutedDispatcher_EmptySeverities_MatchesAll(t *testing.T) {
	m := &mockNotifier{}
	d := NewRoutedDispatcher([]NotifierRoute{
		{Notifier: m}, // no severity filter
	})

	for _, sev := range []string{"P1", "P2", "P3", "", "UNKNOWN"} {
		inc := Incident{Severity: sev, Alerts: []Alert{{Service: "svc"}}, Services: []string{"svc"}}
		d.Dispatch(context.Background(), inc)
	}
	if got := len(m.getIncidents()); got != 5 {
		t.Errorf("expected 5 incidents (all pass), got %d", got)
	}
}

func TestRoutedDispatcher_NoMatch_Skips(t *testing.T) {
	m := &mockNotifier{}
	d := NewRoutedDispatcher([]NotifierRoute{
		{Notifier: m, Severities: []string{"P1"}},
	})

	inc := Incident{Severity: "P3", Alerts: []Alert{{Service: "svc"}}, Services: []string{"svc"}}
	if err := d.Dispatch(context.Background(), inc); err != nil {
		t.Fatal(err)
	}
	if len(m.getIncidents()) != 0 {
		t.Errorf("P3 should be skipped by P1-only route, got %d", len(m.getIncidents()))
	}
}

func TestRoutedDispatcher_MixedRoutes(t *testing.T) {
	pager := &mockNotifier{}
	logCh := &mockNotifier{}
	d := NewRoutedDispatcher([]NotifierRoute{
		{Notifier: pager, Severities: []string{"P1"}},
		{Notifier: logCh}, // all severities
	})

	p1 := Incident{Severity: "P1", Alerts: []Alert{{Service: "svc"}}, Services: []string{"svc"}}
	p3 := Incident{Severity: "P3", Alerts: []Alert{{Service: "svc"}}, Services: []string{"svc"}}

	d.Dispatch(context.Background(), p1)
	d.Dispatch(context.Background(), p3)

	if got := len(pager.getIncidents()); got != 1 {
		t.Errorf("pager: expected 1 (P1 only), got %d", got)
	}
	if got := len(logCh.getIncidents()); got != 2 {
		t.Errorf("log: expected 2 (all), got %d", got)
	}
}

func TestRoutedDispatcher_EventTypePassedThrough(t *testing.T) {
	m := &mockNotifier{}
	d := NewRoutedDispatcher([]NotifierRoute{
		{Notifier: m},
	})

	inc := Incident{
		Severity:  "P1",
		EventType: "resolved",
		Status:    StatusResolved,
		Alerts:    []Alert{{Service: "svc"}},
		Services:  []string{"svc"},
	}
	d.Dispatch(context.Background(), inc)

	got := m.getIncidents()
	if len(got) != 1 {
		t.Fatalf("expected 1 incident, got %d", len(got))
	}
	if got[0].EventType != "resolved" {
		t.Errorf("EventType = %q, want resolved", got[0].EventType)
	}
	if got[0].Status != StatusResolved {
		t.Errorf("Status = %q, want RESOLVED", got[0].Status)
	}
}

func TestRoutedDispatcher_NoSeverityOnIncident(t *testing.T) {
	m := &mockNotifier{}
	d := NewRoutedDispatcher([]NotifierRoute{
		{Notifier: m, Severities: []string{"P1"}},
	})

	inc := Incident{Alerts: []Alert{{Service: "svc"}}, Services: []string{"svc"}}
	d.Dispatch(context.Background(), inc)

	if len(m.getIncidents()) != 0 {
		t.Error("incident with no severity should not match P1-only route")
	}
}
