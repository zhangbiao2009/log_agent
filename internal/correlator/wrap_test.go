package correlator

import (
	"context"
	"testing"
	"time"

	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/notify"
)

func TestWrapAlerts_SingleAlert(t *testing.T) {
	in := make(chan notify.Alert, 1)
	out := WrapAlerts(context.Background(), in)
	alert := notify.Alert{Service: "svc-A", Level: "ERROR", Count: 5, Window: time.Minute, Timestamp: time.Now()}
	in <- alert
	close(in)
	var incs []notify.Incident
	for inc := range out {
		incs = append(incs, inc)
	}
	if len(incs) != 1 {
		t.Fatalf("got %d incidents, want 1", len(incs))
	}
	inc := incs[0]
	if len(inc.Alerts) != 1 {
		t.Errorf("len(Alerts) = %d, want 1", len(inc.Alerts))
	}
	if inc.RootService != "" {
		t.Errorf("RootService = %q, want empty", inc.RootService)
	}
	if inc.DepChain != nil {
		t.Errorf("DepChain = %v, want nil", inc.DepChain)
	}
	if len(inc.Services) != 1 || inc.Services[0] != "svc-A" {
		t.Errorf("Services = %v, want [svc-A]", inc.Services)
	}
}

func TestWrapAlerts_MultipleAlerts(t *testing.T) {
	in := make(chan notify.Alert, 3)
	out := WrapAlerts(context.Background(), in)
	for _, svc := range []string{"svc-A", "svc-B", "svc-C"} {
		in <- notify.Alert{Service: svc, Level: "ERROR", Count: 1, Window: time.Minute, Timestamp: time.Now()}
	}
	close(in)
	var incs []notify.Incident
	for inc := range out {
		incs = append(incs, inc)
	}
	if len(incs) != 3 {
		t.Fatalf("got %d incidents, want 3 (no grouping)", len(incs))
	}
	// Verify each is a single-alert incident.
	for i, inc := range incs {
		if len(inc.Alerts) != 1 {
			t.Errorf("incident %d: len(Alerts) = %d, want 1", i, len(inc.Alerts))
		}
	}
}

func TestWrapAlerts_ClosesOnInputClose(t *testing.T) {
	in := make(chan notify.Alert)
	out := WrapAlerts(context.Background(), in)
	close(in)
	select {
	case _, ok := <-out:
		if ok {
			t.Error("expected closed channel, got incident")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("output not closed after input close")
	}
}
