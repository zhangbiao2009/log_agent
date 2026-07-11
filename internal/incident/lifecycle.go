package incident

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/core"
)

// LifecycleConfig controls dedup and auto-resolve behavior.
type LifecycleConfig struct {
	DedupWindow   time.Duration // minimum interval between notifications for same incident
	ResolveAfter  time.Duration // auto-resolve after no new events for this duration
	CheckInterval time.Duration // how often to scan for auto-resolve candidates
}

// trackedIncident is internal bookkeeping for an active incident.
type trackedIncident struct {
	incident     core.Incident
	status       core.IncidentStatus
	firstSeen    time.Time
	lastSeen     time.Time
	lastNotified time.Time
	updateCount  int
}

// LifecycleManager tracks incidents through OPEN → ONGOING → RESOLVED states,
// deduplicates notifications, and auto-resolves stale incidents.
type LifecycleManager struct {
	cfg     LifecycleConfig
	now     func() time.Time // injectable clock for testing
	mu      sync.Mutex
	tracked map[string]*trackedIncident
}

// NewLifecycleManager creates a LifecycleManager with the given config.
func NewLifecycleManager(cfg LifecycleConfig) *LifecycleManager {
	return &LifecycleManager{
		cfg:     cfg,
		now:     time.Now,
		tracked: make(map[string]*trackedIncident),
	}
}

// Run consumes incidents from upstream and produces incidents with
// Status/EventType/Duration set. It runs a background goroutine for
// auto-resolve checks. The output channel is closed after all tracked
// incidents are resolved on shutdown.
func (lm *LifecycleManager) Run(ctx context.Context, in <-chan core.Incident) <-chan core.Incident {
	out := make(chan core.Incident)

	go func() {
		defer close(out)

		ticker := time.NewTicker(lm.cfg.CheckInterval)
		defer ticker.Stop()

		for {
			select {
			case inc, ok := <-in:
				if !ok {
					// Upstream closed. Resolve all remaining and exit.
					lm.resolveAll(ctx, out)
					return
				}
				if n, emit := lm.processIncident(inc); emit {
					select {
					case out <- n:
					case <-ctx.Done():
						lm.resolveAll(ctx, out)
						return
					}
				}

			case <-ticker.C:
				lm.checkAutoResolve(ctx, out)

			case <-ctx.Done():
				lm.resolveAll(ctx, out)
				return
			}
		}
	}()

	return out
}

// processIncident handles state transitions for an incoming incident.
// Returns the notification to emit and whether to emit it.
func (lm *LifecycleManager) processIncident(inc core.Incident) (core.Incident, bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := lm.now()

	tracked, exists := lm.tracked[inc.ID]
	if !exists {
		// New incident → OPEN
		lm.tracked[inc.ID] = &trackedIncident{
			incident:     inc,
			status:       core.StatusOpen,
			firstSeen:    now,
			lastSeen:     now,
			lastNotified: now,
			updateCount:  0,
		}
		slog.Info("incident state change", "id", inc.ID, "from", "none", "to", core.StatusOpen, "event", "opened")

		inc.Status = core.StatusOpen
		inc.EventType = "opened"
		inc.OpenedAt = now
		return inc, true
	}

	// Existing incident — update tracking.
	tracked.lastSeen = now
	tracked.incident = inc // replace with latest data
	tracked.updateCount++

	// Check dedup window.
	if now.Sub(tracked.lastNotified) < lm.cfg.DedupWindow {
		slog.Debug("notification suppressed (dedup)", "id", inc.ID, "within", lm.cfg.DedupWindow)
		return core.Incident{}, false
	}

	// Past dedup window → transition to ONGOING and emit "updated".
	oldStatus := tracked.status
	tracked.status = core.StatusOngoing
	tracked.lastNotified = now
	if oldStatus != core.StatusOngoing {
		slog.Info("incident state change", "id", inc.ID, "from", oldStatus, "to", core.StatusOngoing, "event", "updated")
	}

	inc.Status = core.StatusOngoing
	inc.EventType = "updated"
	inc.OpenedAt = tracked.firstSeen
	return inc, true
}

// checkAutoResolve scans tracked incidents and resolves any whose lastSeen
// is older than ResolveAfter.
func (lm *LifecycleManager) checkAutoResolve(ctx context.Context, out chan<- core.Incident) {
	lm.mu.Lock()

	now := lm.now()
	var toResolve []string
	for id, t := range lm.tracked {
		if now.Sub(t.lastSeen) >= lm.cfg.ResolveAfter {
			toResolve = append(toResolve, id)
		}
	}

	var resolved []core.Incident
	for _, id := range toResolve {
		t := lm.tracked[id]
		inc := t.incident
		inc.Status = core.StatusResolved
		inc.EventType = "resolved"
		inc.OpenedAt = t.firstSeen
		inc.Duration = now.Sub(t.firstSeen)
		resolved = append(resolved, inc)
		slog.Info("incident auto-resolved", "id", id, "duration", inc.Duration)
		delete(lm.tracked, id)
	}

	lm.mu.Unlock()

	for _, inc := range resolved {
		select {
		case out <- inc:
		case <-ctx.Done():
			return
		}
	}
}

// resolveAll resolves all tracked incidents (used during shutdown).
// Does not check ctx — shutdown must complete.
func (lm *LifecycleManager) resolveAll(_ context.Context, out chan<- core.Incident) {
	lm.mu.Lock()

	now := lm.now()
	var resolved []core.Incident
	for id, t := range lm.tracked {
		inc := t.incident
		inc.Status = core.StatusResolved
		inc.EventType = "resolved"
		inc.OpenedAt = t.firstSeen
		inc.Duration = now.Sub(t.firstSeen)
		resolved = append(resolved, inc)
		slog.Info("incident resolved (shutdown)", "id", id, "duration", inc.Duration)
		delete(lm.tracked, id)
	}

	lm.mu.Unlock()

	for _, inc := range resolved {
		out <- inc
	}
}
