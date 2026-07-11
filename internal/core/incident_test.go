package core

import (
	"testing"
	"time"
)

func TestIncident_IDDeterministic(t *testing.T) {
	ts := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	window := 5 * time.Second
	id1 := GenerateIncidentID([]string{"svc-A", "svc-B"}, ts, window)
	id2 := GenerateIncidentID([]string{"svc-A", "svc-B"}, ts, window)
	if id1 != id2 {
		t.Errorf("IDs not deterministic: %q != %q", id1, id2)
	}
}

func TestIncident_IDDifferentServices(t *testing.T) {
	ts := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	window := 5 * time.Second
	id1 := GenerateIncidentID([]string{"svc-A", "svc-B"}, ts, window)
	id2 := GenerateIncidentID([]string{"svc-A", "svc-C"}, ts, window)
	if id1 == id2 {
		t.Errorf("different services produced same ID: %q", id1)
	}
}

func TestIncident_IDOrderIndependent(t *testing.T) {
	ts := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	window := 5 * time.Second
	id1 := GenerateIncidentID([]string{"svc-b", "svc-a"}, ts, window)
	id2 := GenerateIncidentID([]string{"svc-a", "svc-b"}, ts, window)
	if id1 != id2 {
		t.Errorf("order-dependent IDs: %q != %q", id1, id2)
	}
}

func TestIncident_SingleAlertIncident(t *testing.T) {
	inc := Incident{
		Services: []string{"svc-A"},
		Alerts:   []Alert{{Service: "svc-A", Level: "ERROR", Count: 5}},
	}
	if !inc.IsSingleAlert() {
		t.Error("IsSingleAlert() = false, want true")
	}
	if inc.RootService != "" {
		t.Errorf("RootService = %q, want empty", inc.RootService)
	}
	if inc.DepChain != nil {
		t.Errorf("DepChain = %v, want nil", inc.DepChain)
	}
}
