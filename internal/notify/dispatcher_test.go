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
