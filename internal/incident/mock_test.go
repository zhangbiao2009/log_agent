package incident

import (
	"context"
	"sync"

	"github.com/zhangbiao2009/log_agent/internal/core"
)

// mockNotifier is a test double implementing notify.Notifier, used by the
// lifecycle→dispatcher integration test in this package.
type mockNotifier struct {
	sendFunc  func(ctx context.Context, inc core.Incident) error
	incidents []core.Incident
	mu        sync.Mutex
}

func (m *mockNotifier) Send(ctx context.Context, inc core.Incident) error {
	m.mu.Lock()
	m.incidents = append(m.incidents, inc)
	m.mu.Unlock()
	if m.sendFunc != nil {
		return m.sendFunc(ctx, inc)
	}
	return nil
}

func (m *mockNotifier) Name() string { return "mock" }

func (m *mockNotifier) getIncidents() []core.Incident {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]core.Incident, len(m.incidents))
	copy(cp, m.incidents)
	return cp
}
