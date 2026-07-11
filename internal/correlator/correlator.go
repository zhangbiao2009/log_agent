package correlator

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/core"
)

// CorrelatorConfig controls correlation behavior.
type CorrelatorConfig struct {
	Window time.Duration // default: 2min
}

// Correlator buffers anomalous alerts and groups them into incidents
// using the dependency graph.
type Correlator struct {
	config CorrelatorConfig
	graph  *DependencyGraph
	Clock  core.Clock
}

// NewCorrelator creates a Correlator with defaults applied.
func NewCorrelator(cfg CorrelatorConfig, graph *DependencyGraph) *Correlator {
	if cfg.Window == 0 {
		cfg.Window = 2 * time.Minute
	}
	return &Correlator{
		config: cfg,
		graph:  graph,
		Clock:  realClock{},
	}
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Run consumes anomalous alerts and emits correlated incidents.
// Flush happens on every Window tick or when the input channel closes.
func (c *Correlator) Run(ctx context.Context, in <-chan core.Alert) <-chan core.Incident {
	out := make(chan core.Incident, 100)
	go func() {
		defer close(out)
		var buffer []core.Alert
		timer := c.Clock.After(c.config.Window)
		for {
			select {
			case <-ctx.Done():
				c.flush(buffer, out)
				return
			case <-timer:
				c.flush(buffer, out)
				buffer = nil
				timer = c.Clock.After(c.config.Window)
			case alert, ok := <-in:
				if !ok {
					c.flush(buffer, out)
					return
				}
				buffer = append(buffer, alert)
			}
		}
	}()
	return out
}

// flush groups buffered alerts by connected component and emits incidents.
func (c *Correlator) flush(alerts []core.Alert, out chan<- core.Incident) {
	if len(alerts) == 0 {
		return
	}

	now := c.Clock.Now()

	// Group alerts by component.
	type group struct {
		alerts []core.Alert
	}
	groups := make(map[int]*group)
	unknownID := -1

	for _, a := range alerts {
		compID := c.graph.Component(a.Service)
		if compID < 0 {
			// Service not in graph → its own group.
			groups[unknownID] = &group{alerts: []core.Alert{a}}
			unknownID--
			continue
		}
		g, ok := groups[compID]
		if !ok {
			g = &group{}
			groups[compID] = g
		}
		g.alerts = append(g.alerts, a)
	}

	for _, g := range groups {
		inc := c.buildIncident(g.alerts, now)
		out <- inc
	}
}

// buildIncident creates an Incident from a group of correlated alerts.
func (c *Correlator) buildIncident(alerts []core.Alert, now time.Time) core.Incident {
	// Collect services and find earliest timestamp.
	serviceSet := make(map[string]bool)
	var earliest time.Time
	for _, a := range alerts {
		serviceSet[a.Service] = true
		if earliest.IsZero() || a.Timestamp.Before(earliest) {
			earliest = a.Timestamp
		}
	}

	services := make([]string, 0, len(serviceSet))
	for s := range serviceSet {
		services = append(services, s)
	}
	sort.Strings(services)

	// Single service not in graph → no correlation metadata.
	if len(services) == 1 && c.graph.Component(services[0]) < 0 {
		return core.Incident{
			Alerts:   alerts,
			Services: services,
			OpenedAt: earliest,
			Window:   c.config.Window,
		}
	}

	// Find root cause: deepest service by BFS depth.
	// Tie-break by max ZScore, then alphabetically.
	alertByService := make(map[string]core.Alert)
	for _, a := range alerts {
		alertByService[a.Service] = a
	}

	root := services[0]
	rootDepth := c.graph.Depth(root)
	rootZScore := maxZScore(alertByService[root])
	for _, svc := range services[1:] {
		d := c.graph.Depth(svc)
		z := maxZScore(alertByService[svc])
		if d > rootDepth || (d == rootDepth && z > rootZScore) || (d == rootDepth && z == rootZScore && svc < root) {
			root = svc
			rootDepth = d
			rootZScore = z
		}
	}

	// Build dep chain: sort by depth descending, ties alphabetically.
	type svcDepth struct {
		svc   string
		depth int
	}
	sd := make([]svcDepth, len(services))
	for i, s := range services {
		sd[i] = svcDepth{s, c.graph.Depth(s)}
	}
	sort.Slice(sd, func(i, j int) bool {
		if sd[i].depth != sd[j].depth {
			return sd[i].depth > sd[j].depth
		}
		return sd[i].svc < sd[j].svc
	})
	depChain := make([]string, len(sd))
	for i, s := range sd {
		depChain[i] = s.svc
	}

	id := core.GenerateIncidentID(services, earliest, c.config.Window)

	slog.Info("incident created",
		"id", id,
		"root", root,
		"services", services,
		"dep_chain", depChain,
	)

	return core.Incident{
		ID:          id,
		Services:    services,
		RootService: root,
		DepChain:    depChain,
		Alerts:      alerts,
		OpenedAt:    earliest,
		Window:      c.config.Window,
	}
}

// maxZScore returns the maximum ZScore across all patterns in an alert.
func maxZScore(a core.Alert) float64 {
	var max float64
	for _, p := range a.Patterns {
		if p.ZScore > max {
			max = p.ZScore
		}
	}
	return max
}
