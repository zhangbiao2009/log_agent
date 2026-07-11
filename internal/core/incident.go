package core

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"
)

// IncidentStatus represents the lifecycle state of an incident.
type IncidentStatus string

const (
	StatusOpen     IncidentStatus = "OPEN"
	StatusOngoing  IncidentStatus = "ONGOING"
	StatusResolved IncidentStatus = "RESOLVED"
)

// Incident groups correlated alerts from related services.
// Single-alert Incidents (from WrapAlerts or uncorrelated services) have
// RootService="" and len(Alerts)==1; renderers treat them identically to
// Phase 3 alerts.
type Incident struct {
	ID          string        // deterministic hash of sorted service names + window
	Services    []string      // all affected services (sorted)
	RootService string        // suspected root cause (deepest in dep chain)
	DepChain    []string      // affected services sorted by depth descending
	Alerts      []Alert       // all correlated alerts in this incident
	OpenedAt    time.Time     // timestamp of earliest alert
	Window      time.Duration // correlation window used

	// Set by Diagnoser (Phase 5). Zero values when diagnoser is disabled.
	Diagnosis   string   // LLM-generated root-cause explanation
	Severity    string   // P1, P2, P3 (assigned by LLM or heuristic)
	Suggestions []string // actionable fix steps

	// Set by LifecycleManager (Phase 6). Zero values when lifecycle is disabled.
	Status    IncidentStatus // OPEN, ONGOING, RESOLVED
	EventType string         // "opened", "updated", "resolved"
	Duration  time.Duration  // elapsed since OpenedAt (set on resolve)
}

// IsSingleAlert returns true for uncorrelated single-alert incidents
// that should render identically to Phase 3 format.
func (inc Incident) IsSingleAlert() bool {
	return len(inc.Alerts) == 1 && inc.RootService == ""
}

// GenerateIncidentID produces a deterministic 12-char hex ID from sorted
// service names and the window-truncated open time.
func GenerateIncidentID(services []string, openedAt time.Time, window time.Duration) string {
	sorted := make([]string, len(services))
	copy(sorted, services)
	sort.Strings(sorted)

	floor := openedAt.Truncate(window)
	payload := strings.Join(sorted, ",") + "|" + floor.UTC().Format(time.RFC3339Nano)
	h := sha256.Sum256([]byte(payload))
	return fmt.Sprintf("%x", h[:6])
}
