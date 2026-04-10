package notify

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type mockNotifier struct {
	sendFunc func(ctx context.Context, a Alert) error
	alerts   []Alert
	mu       sync.Mutex
}

func (m *mockNotifier) Send(ctx context.Context, a Alert) error {
	m.mu.Lock()
	m.alerts = append(m.alerts, a)
	m.mu.Unlock()
	if m.sendFunc != nil {
		return m.sendFunc(ctx, a)
	}
	return nil
}

func (m *mockNotifier) Name() string { return "mock" }

func (m *mockNotifier) getAlerts() []Alert {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]Alert, len(m.alerts))
	copy(cp, m.alerts)
	return cp
}

func TestDispatcher_AllSuccess(t *testing.T) {
	m1 := &mockNotifier{}
	m2 := &mockNotifier{}
	d := NewDispatcher(m1, m2)

	alert := Alert{Service: "svc1", Level: "ERROR", Count: 5}
	err := d.Dispatch(context.Background(), alert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(m1.getAlerts()) != 1 {
		t.Errorf("m1 got %d alerts, want 1", len(m1.getAlerts()))
	}
	if len(m2.getAlerts()) != 1 {
		t.Errorf("m2 got %d alerts, want 1", len(m2.getAlerts()))
	}
}

func TestDispatcher_PartialFailure(t *testing.T) {
	m1 := &mockNotifier{}
	m2 := &mockNotifier{
		sendFunc: func(ctx context.Context, a Alert) error {
			return errors.New("slack down")
		},
	}
	d := NewDispatcher(m1, m2)

	alert := Alert{Service: "svc1", Level: "ERROR", Count: 5}
	err := d.Dispatch(context.Background(), alert)
	if err == nil {
		t.Fatal("expected error from partial failure")
	}

	if len(m1.getAlerts()) != 1 {
		t.Errorf("m1 got %d alerts, want 1", len(m1.getAlerts()))
	}
}

func TestDispatcher_NoNotifiers(t *testing.T) {
	d := NewDispatcher()
	err := d.Dispatch(context.Background(), Alert{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDispatcher_AllFail(t *testing.T) {
	fail := func(ctx context.Context, a Alert) error {
		return errors.New("fail")
	}
	m1 := &mockNotifier{sendFunc: fail}
	m2 := &mockNotifier{sendFunc: fail}
	d := NewDispatcher(m1, m2)

	err := d.Dispatch(context.Background(), Alert{Service: "svc1"})
	if err == nil {
		t.Fatal("expected error when all notifiers fail")
	}
}
