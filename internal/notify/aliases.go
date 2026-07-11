package notify

import "github.com/zhangbiao2009/log_agent/internal/core"

// The pipeline's shared domain types now live in internal/core so that
// upstream stages (anomaly, correlator, diagnosis) don't have to import the
// notify package. These aliases keep the notify.* names working for the
// notifier backends and existing call sites.
type (
	Alert          = core.Alert
	PatternSummary = core.PatternSummary
	AnomalyKind    = core.AnomalyKind
	Incident       = core.Incident
	IncidentStatus = core.IncidentStatus
	Clock          = core.Clock
)

const (
	AnomalyNone       = core.AnomalyNone
	AnomalyNewPattern = core.AnomalyNewPattern
	AnomalySpike      = core.AnomalySpike
	AnomalyRateJump   = core.AnomalyRateJump

	StatusOpen     = core.StatusOpen
	StatusOngoing  = core.StatusOngoing
	StatusResolved = core.StatusResolved
)

// GenerateIncidentID is re-exported from core for existing call sites.
var GenerateIncidentID = core.GenerateIncidentID
