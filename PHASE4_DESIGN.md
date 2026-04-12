# Phase 4: Cross-Service Correlator — Design Document

**Layer:** L4 (Architecture diagram)  
**Date:** April 12, 2026  
**Status:** Draft  
**Depends on:** Phase 3 (Anomaly Detection)

---

## 1. Goal

Turn isolated per-service anomaly alerts into **incidents** by grouping
co-occurring anomalies from related services. Identify the suspected root
cause using the service dependency graph.

**Before (Phase 3):** Three separate alerts —  
"payment-service: connection refused (SPIKE)", "order-service: timeout
(SPIKE)", "notification-service: queue full (NEW)".

**After (Phase 4):** One incident —  
"payment-service is the suspected root cause (deepest in chain).
order-service and notification-service are cascading. Dependency chain:
order-service → payment-service."

> **Note on roadmap ordering:** The original DESIGN.md roadmap labeled
> Phase 4 as "LLM Diagnosis," combining L4 and L5. This document
> implements L4 (Correlator) as its own phase because it is a self-contained
> Go pipeline stage with no external dependencies, while L5 (LLM Diagnosis)
> requires API integration and RAG. L5 becomes Phase 5.

---

## 2. Architecture

```
                    ┌─────────────────────┐
  anomalous alerts  │  L4: Correlator     │  incidents
  ────────────────► │                     │ ──────────────►
  <-chan Alert       │  1. Buffer alerts   │  <-chan Incident
                    │  2. Group by graph  │
                    │  3. Find root cause │
                    └─────────────────────┘
```

The correlator is a **channel pipeline stage** that sits between the
AnomalyDetector and the Dispatcher, following the same pattern as every
other L2-L3 stage.

### Pipeline (with correlator enabled)

```
Source → Filter → Pattern → Aggregator → AnomalyDetector → Correlator → Dispatcher
        <-chan LogLine                   <-chan Alert        <-chan Incident
```

### Pipeline (correlator disabled)

```
Source → Filter → Pattern → Aggregator → AnomalyDetector → Wrap → Dispatcher
        <-chan LogLine                   <-chan Alert        <-chan Incident
```

Each Alert is wrapped in a single-alert Incident so the Dispatcher always
receives `Incident` objects regardless of configuration.

---

## 3. Data Types

### 3.1 Incident

The core output type. Lives in `internal/correlator/`.

```go
type Incident struct {
    ID          string         // deterministic hash of sorted service names + window
    Services    []string       // all affected services (sorted)
    RootService string         // suspected root cause (deepest in dep chain)
    DepChain    []string       // dependency path: root → ... → furthest affected
    Alerts      []notify.Alert // all correlated alerts in this incident
    OpenedAt    time.Time      // timestamp of earliest alert
    Window      time.Duration  // correlation window used
}
```

**ID generation:** `sha256(sorted_services + window_start)[:12]` — deterministic,
so the same set of services in the same window always produces the same ID.
This supports future deduplication (L6).

### 3.2 DependencyGraph

Loaded from a static YAML config file. Represents directed "calls" edges.

```go
type DependencyGraph struct {
    edges map[string][]string // service → services it calls
}
```

**API:**

| Method | Signature | Purpose |
|---|---|---|
| `Calls` | `(svc) []string` | Direct downstream dependencies |
| `CalledBy` | `(svc) []string` | Direct upstream dependents |
| `Connected` | `(a, b) bool` | Reachability in either direction |
| `Depth` | `(svc) int` | Max depth from any root (0 = root/no callers) |
| `ShortestPath` | `(from, to) []string` | Shortest directed path, nil if unreachable |

**YAML format** (same as DESIGN.md):

```yaml
# config/dependencies.yaml
services:
  order-service:
    calls: [payment-service, inventory-service, notification-service]
  payment-service:
    calls: [bank-gateway, fraud-detection]
  inventory-service:
    calls: [warehouse-db]
  notification-service:
    calls: [email-provider, sms-provider]
```

---

## 4. Correlation Algorithm

### 4.1 Buffering

The correlator accumulates anomalous alerts for a configurable **correlation
window** (default: 2 minutes). This is a wall-clock timer, same as the
aggregator window.

```
time ──────────────────────────────────────────────────►
     │◄──── correlation window (2min) ────►│
     │                                      │
     │ alert A (svc-1)                      │
     │       alert B (svc-2)                │  ← same incident
     │              alert C (svc-3)         │
     │                                      │
     │                                 FLUSH│
```

On flush:
1. Collect all buffered alerts into a set.
2. Group into connected components using the dependency graph.
3. Emit one `Incident` per group.
4. Alerts from services not in the dependency graph → each becomes its own
   single-alert Incident (no correlation data).

### 4.2 Grouping

**Algorithm:** Union-Find on buffered alert services.

```
For each pair (A, B) of services with alerts in the buffer:
    if graph.Connected(A, B):
        union(A, B)

Each resulting group → one Incident
```

`Connected(A, B)` returns true if there is a directed path from A to B **or**
from B to A (bidirectional reachability). This handles the case where the
root cause is deeper than the alerting service.

### 4.3 Root Cause Heuristic

Among all services in an incident group, the **root service** is the one
with the greatest `Depth()` in the dependency graph — i.e., the service
furthest from root-level entry points.

**Rationale:** Errors cascade *upstream*. If payment-service depends on
bank-gateway and both are erroring, bank-gateway is more likely the root
cause.

**Tie-breaking:** If multiple services share the same depth, pick the one
whose alert has the highest ZScore (strongest anomaly signal).

### 4.4 Dependency Chain Construction

After selecting the root service, build the dependency chain:
- Start from the root service.
- Find shortest paths from the root to every other affected service
  (reversed edges — root is deepest, chain goes toward entry points).
- Emit as a flattened path: `[root, intermediate1, ..., entry-point]`.

If no direct path exists (services connected through a shared ancestor),
list all services sorted by depth (deepest first).

---

## 5. Pipeline Stage API

```go
// CorrelatorConfig controls correlation behavior.
type CorrelatorConfig struct {
    Window time.Duration // default: 2min; how long to buffer alerts
}

type Correlator struct {
    config CorrelatorConfig
    graph  *DependencyGraph
    Clock  notify.Clock
}

func NewCorrelator(cfg CorrelatorConfig, graph *DependencyGraph) *Correlator

// Run consumes anomalous alerts and emits correlated incidents.
// Flush happens on every Window tick or when the input channel closes.
func (c *Correlator) Run(ctx context.Context, in <-chan notify.Alert) <-chan Incident

// WrapAlerts is the bypass path when the correlator is disabled.
// Each alert becomes a single-alert Incident with no correlation metadata.
func WrapAlerts(ctx context.Context, in <-chan notify.Alert) <-chan Incident
```

### 5.1 Flush Triggers

| Trigger | Behavior |
|---|---|
| Window timer fires | Flush all buffered alerts, emit incidents |
| Input channel closes | Final flush of remaining buffer, close output |
| Context cancelled | Close output immediately |

This matches the Aggregator's flush model exactly.

---

## 6. Notification Changes

The Dispatcher and Notifiers currently operate on `notify.Alert`. Phase 4
changes them to operate on `correlator.Incident`.

### 6.1 Dispatcher

```go
// Before (Phase 3):
func (d *Dispatcher) Dispatch(ctx context.Context, alert Alert) error

// After (Phase 4):
func (d *Dispatcher) Dispatch(ctx context.Context, incident correlator.Incident) error
```

### 6.2 Notifier Interface

```go
// Before (Phase 3):
type Notifier interface {
    Send(ctx context.Context, alert Alert) error
    Name() string
}

// After (Phase 4):
type Notifier interface {
    Send(ctx context.Context, incident correlator.Incident) error
    Name() string
}
```

### 6.3 Rendering Changes

**LogNotifier** — render incident header then each alert's patterns:

```
INCIDENT inc-a3f2 | root: bank-gateway | services: order-service, payment-service, bank-gateway
  chain: bank-gateway → payment-service → order-service
  [bank-gateway] 0 errors — service appears DOWN
  [payment-service] 200x ERROR connection refused to <*>:443 [SPIKE z=12.3]
  [order-service] 50x ERROR timeout calling payment-service <*> [SPIKE z=8.1]
```

**SlackNotifier** — incident header block + per-service pattern blocks:

```
🔴 INCIDENT inc-a3f2 — 3 services affected
Root cause: bank-gateway (deepest in chain)
Chain: bank-gateway → payment-service → order-service

[payment-service] ...pattern blocks...
[order-service] ...pattern blocks...
```

**Single-alert Incidents** (correlator disabled): Render identically to
the current Phase 3 format — the incident header is omitted when
`len(incident.Alerts) == 1 && incident.RootService == ""`.

---

## 7. Configuration

```yaml
# config/config.yaml additions
correlator:
  enabled: true
  window: 2m
  dependencies_file: config/dependencies.yaml
```

```go
// main.go additions
type CorrelatorConfig struct {
    Enabled          bool   `yaml:"enabled"`
    Window           string `yaml:"window"`
    DependenciesFile string `yaml:"dependencies_file"`
}
```

When `correlator.enabled: false`, the pipeline uses `WrapAlerts()` to
bypass correlation entirely. All existing behavior is preserved.

---

## 8. Package Structure

```
internal/correlator/
    depgraph.go        — DependencyGraph (load YAML, Connected, Depth, etc.)
    correlator.go      — Correlator pipeline stage + CorrelatorConfig
    incident.go        — Incident type + ID generation
    wrap.go            — WrapAlerts bypass function
config/
    dependencies.yaml  — static service dependency graph
```

---

## 9. Edge Cases

| Scenario | Behavior |
|---|---|
| Single service alert, no graph edges | Emitted as single-alert Incident (no root cause) |
| Service not in dependency graph | Treated as isolated; own Incident with no dep chain |
| All alerts from one service | Single-service Incident; root = that service |
| Transitive dependency (A→B→C, A+C alert but not B) | A and C grouped (Connected via B); B listed in dep chain even though it didn't alert |
| Two disconnected groups alert simultaneously | Two separate Incidents emitted |
| No alerts in a window | No Incidents emitted (empty flush) |
| Circular dependency in graph | Connected returns true for all cycle members; Depth uses max acyclic depth (BFS) |

---

## 10. What Phase 4 Does NOT Do

| Concern | Deferred to |
|---|---|
| LLM root-cause diagnosis | Phase 5 (L5) |
| Incident lifecycle (OPEN → ONGOING → RESOLVED) | Phase 5/6 |
| Deduplication across windows | Phase 6 |
| Auto-discovered dependency graph (from traces) | Future enhancement |
| SQLite persistence for incidents | Phase 5 |
| Severity assignment (P1/P2/P3) | Phase 5 (LLM assigns severity) |

---

## 11. Implementation Plan

1. **`depgraph.go`** — Dependency graph loader + query methods.
2. **`incident.go`** — Incident struct + ID generation.
3. **`correlator.go`** — Pipeline stage with buffer + flush + grouping.
4. **`wrap.go`** — WrapAlerts bypass.
5. **Update `notify/`** — Change Notifier interface to accept Incident.
   Update LogNotifier and SlackNotifier rendering.
6. **Update `cmd/agent/main.go`** — Add correlator wiring + config.
7. **Create `config/dependencies.yaml`** — Sample dependency graph.
8. **Update `testdata/sample_logs.ndjson`** — Multi-service fixture
   with dependent services alerting in the same window.
