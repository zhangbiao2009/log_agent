# Phase 4: Cross-Service Correlator — Test Plan

**Scope:** Unit tests for `internal/correlator`, updated tests for
`internal/notify` (Notifier interface change), and an integration
smoke test for the correlator pipeline with multi-service alerts.

---

## 1. Guiding Principles

- **No real time.** All tests use an injectable `Clock` (same `notify.Clock`
  interface). Correlation window ticks are driven by `FakeClock.Advance`.
- **Small, deterministic graphs.** Tests use hand-crafted 3-5 service
  dependency graphs, not large production-scale configs.
- **Backward compatibility.** When the correlator is disabled
  (`WrapAlerts`), all downstream behavior must be identical to Phase 3.
  Existing Phase 3 tests must continue to pass after the Notifier
  interface change (Incident wraps Alert).
- **Race detector.** All tests run with `-race`.

---

## 2. Test Files

```
internal/correlator/
    depgraph_test.go     — DependencyGraph unit tests (14 cases)
    incident_test.go     — Incident ID + helpers (4 cases)
    correlator_test.go   — Correlator pipeline tests (16 cases)
    wrap_test.go         — WrapAlerts bypass tests (3 cases)
internal/notify/
    log_test.go          — (append) incident rendering tests (3 cases)
    slack_test.go        — (append) incident rendering tests (3 cases)
    dispatcher_test.go   — (update) existing tests for new Incident type
```

---

## 3. `depgraph_test.go` — DependencyGraph (14 cases)

### 3.1 Loading (3 cases)

**`TestDepGraph_LoadFromYAML`**
- Input: valid YAML with 4 services and directed edges.
- Expected: `graph.Calls("order-service")` returns
  `["payment-service", "inventory-service"]` (sorted).
- Rationale: basic loading works.

**`TestDepGraph_LoadEmptyFile`**
- Input: YAML with `services: {}`.
- Expected: empty graph, no error. All queries return empty/zero.
- Rationale: empty config must not panic.

**`TestDepGraph_LoadUnknownServiceReference`**
- Input: `order-service` calls `["missing-service"]` — `missing-service`
  has no own entry.
- Expected: no error. `graph.Calls("order-service")` includes
  `"missing-service"`. `graph.Calls("missing-service")` returns `[]`.
- Rationale: leaf services don't need their own `services:` entry.

### 3.2 Calls / CalledBy (3 cases)

**`TestDepGraph_Calls`**
- Graph: `A → [B, C]`, `B → [D]`.
- `Calls("A")` → `["B", "C"]`. `Calls("D")` → `[]`.
- Rationale: direct edge lookup.

**`TestDepGraph_CalledBy`**
- Same graph. `CalledBy("B")` → `["A"]`. `CalledBy("A")` → `[]`.
- Rationale: reverse edge lookup.

**`TestDepGraph_CalledByUnknownService`**
- `CalledBy("nonexistent")` → `[]`.
- Rationale: unknown services return empty, no panic.

### 3.3 Connected (3 cases)

**`TestDepGraph_ConnectedDirectEdge`**
- `A → B`. `Connected("A", "B")` → `true`. `Connected("B", "A")` → `true`.
- Rationale: bidirectional reachability — error cascades go upstream.

**`TestDepGraph_ConnectedTransitive`**
- `A → B → C`. `Connected("A", "C")` → `true`.
- Rationale: transitive reachability through intermediate services.

**`TestDepGraph_NotConnectedDisjoint`**
- `A → B`, `C → D` (two disconnected components).
- `Connected("A", "D")` → `false`.
- Rationale: disconnected services are not correlated.

### 3.4 Depth (3 cases)

**`TestDepGraph_DepthLinearChain`**
- `A → B → C → D`.
- `Depth("A")` → `0`, `Depth("B")` → `1`, `Depth("C")` → `2`, `Depth("D")` → `3`.
- Rationale: linear chain, depth = position from root.

**`TestDepGraph_DepthDiamond`**
- `A → B`, `A → C`, `B → D`, `C → D`.
- `Depth("D")` → `2` (max of both paths).
- Rationale: diamond dependency, depth = max path length.

**`TestDepGraph_DepthIsolatedService`**
- Service `X` with no edges in or out.
- `Depth("X")` → `0`.
- Rationale: isolated services are at depth 0.

### 3.5 ShortestPath (2 cases)

**`TestDepGraph_ShortestPathExists`**
- `A → B → C`. `ShortestPath("A", "C")` → `["A", "B", "C"]`.
- Rationale: returns full path including endpoints.

**`TestDepGraph_ShortestPathUnreachable`**
- `A → B`, `C → D`. `ShortestPath("A", "D")` → `nil`.
- Rationale: no path between disconnected services.

---

## 4. `incident_test.go` — Incident helpers (4 cases)

**`TestIncident_IDDeterministic`**
- Two Incidents with the same sorted services + same window start time.
- Expected: same ID.
- Rationale: ID generation is deterministic for deduction.

**`TestIncident_IDDifferentServices`**
- Two Incidents with different service sets.
- Expected: different IDs.
- Rationale: distinct incidents get distinct IDs.

**`TestIncident_IDOrderIndependent`**
- Incident A with services `["svc-b", "svc-a"]`.
- Incident B with services `["svc-a", "svc-b"]`.
- Expected: same ID (services are sorted before hashing).
- Rationale: insertion order must not affect ID.

**`TestIncident_SingleAlertIncident`**
- Incident with one alert, no root service, no dep chain.
- Expected: `len(Services) == 1`, `RootService == ""`, `DepChain == nil`.
- Rationale: single-alert incidents (from WrapAlerts) are valid.

---

## 5. `correlator_test.go` — Correlator Pipeline (16 cases)

All correlator tests use helpers:
- `makeAlert(service, patterns...)` — same as Phase 3 detector tests.
- `collectIncidents(out, n, timeout)` — drain output channel.
- `testGraph()` — returns a small graph: `A → [B, C]`, `B → [D]`.

### 5.1 Pipeline Mechanics (3 cases)

**`TestCorrelator_ClosesOutputWhenInputCloses`**
- Close input immediately.
- Expected: output closes (final flush with empty buffer → no incidents).
- Rationale: no goroutine leak.

**`TestCorrelator_ClosesOutputOnContextCancel`**
- Send one alert, cancel ctx.
- Expected: output closes within 1s.
- Rationale: graceful shutdown.

**`TestCorrelator_BufferedOutputMatchesInput`**
- Input buffered at 10. Verify `cap(out) == cap(in)`.
- Rationale: consistent backpressure.

### 5.2 Single-Service Correlation (3 cases)

**`TestCorrelator_SingleAlertBecomesIncident`**
- Send one alert for service `A`, advance clock past window, close input.
- Expected: one Incident with `Services=["A"]`, `len(Alerts)==1`,
  `RootService=""` (no other service to correlate with).
- Rationale: lone alerts are emitted as single-service incidents.

**`TestCorrelator_MultipleAlertsFromSameService`**
- Send 3 alerts for service `A` within one window.
- Expected: one Incident with `Services=["A"]`, `len(Alerts)==3`.
- Rationale: multiple windows of the same service are grouped.

**`TestCorrelator_UnknownServiceNotInGraph`**
- Graph has A→B→C. Send alert for service `X` (not in graph).
- Expected: Incident with `Services=["X"]`, no dep chain.
- Rationale: services outside the graph are isolated.

### 5.3 Multi-Service Correlation (4 cases)

**`TestCorrelator_RelatedServicesGrouped`**
- Graph: `A → B → D`. Alerts for `A` and `D` within one window.
- Expected: one Incident with `Services=["A", "D"]` (or sorted),
  `RootService="D"` (depth 2 > depth 0).
- Rationale: connected services form one incident.

**`TestCorrelator_UnrelatedServicesNotGrouped`**
- Graph: `A → B`, `C → D`. Alerts for `A` and `D` within one window.
- Expected: two separate Incidents.
- Rationale: disconnected services are separate incidents.

**`TestCorrelator_ThreeServiceIncident`**
- Graph: `A → B → C`. Alerts for all three within one window.
- Expected: one Incident, `RootService="C"` (depth 2),
  `DepChain=["C", "B", "A"]` (deepest first).
- Rationale: full chain correlation.

**`TestCorrelator_TransitiveDependencyGrouped`**
- Graph: `A → B → C`. Alerts for `A` and `C` (not `B`).
- Expected: one Incident (A and C connected through B).
  `RootService="C"`.
- Rationale: transitive connectivity via non-alerting intermediate service.

### 5.4 Root Cause Selection (3 cases)

**`TestCorrelator_RootCauseIsDeepestService`**
- Graph: `A → B → C`. Alerts for `A`, `B`, `C`.
- Expected: `RootService="C"` (depth 2).
- Rationale: deepest-in-chain heuristic.

**`TestCorrelator_RootCauseTieBreakByZScore`**
- Graph: `A → B`, `A → C` (B and C at same depth=1).
- Alerts: B with ZScore=5.0, C with ZScore=2.0.
- Expected: `RootService="B"` (higher ZScore wins tie).
- Rationale: strongest anomaly signal breaks ties.

**`TestCorrelator_RootCauseSingleService`**
- Only one service alerts. No root cause analysis needed.
- Expected: `RootService=""`.
- Rationale: no correlation possible for a single service.

### 5.5 Window Behavior (3 cases)

**`TestCorrelator_AlertsInDifferentWindowsSeparated`**
- Send alert for `A`, advance clock past window. Send alert for `B`.
  Advance clock past window again. Close input.
- Expected: two Incidents (one per window).
- Rationale: correlation window boundaries separate incidents.

**`TestCorrelator_FlushOnInputClose`**
- Send two related alerts, close input without advancing window.
- Expected: final flush emits one correlated Incident.
- Rationale: partial window flushes on shutdown (same as Aggregator).

**`TestCorrelator_EmptyWindowNoIncident`**
- Advance clock past window with no alerts buffered.
- Expected: no Incidents emitted.
- Rationale: empty flushes produce nothing.

---

## 6. `wrap_test.go` — WrapAlerts Bypass (3 cases)

**`TestWrapAlerts_SingleAlert`**
- Send one alert through `WrapAlerts`.
- Expected: one Incident with `len(Alerts)==1`, `RootService==""`,
  `DepChain==nil`, `Services==[alert.Service]`.
- Rationale: trivial wrapping preserves alert data.

**`TestWrapAlerts_MultipleAlerts`**
- Send 3 alerts through `WrapAlerts`.
- Expected: 3 Incidents, one per alert (no grouping).
- Rationale: bypass mode does not correlate.

**`TestWrapAlerts_ClosesOnInputClose`**
- Close input. Expected: output closes.
- Rationale: no goroutine leak.

---

## 7. Notify Tests (updated for Incident)

### 7.1 `log_test.go` — Incident rendering (append 3 cases)

**`TestLogNotifier_MultiServiceIncident`**
- Incident with 2 alerts (svc-A, svc-B), RootService="svc-B", DepChain set.
- Expected: output contains "INCIDENT", "root: svc-B", both service names.
- Rationale: multi-service incidents show correlation data.

**`TestLogNotifier_SingleAlertIncidentRendersAsAlert`**
- Incident with 1 alert, RootService="".
- Expected: output matches Phase 3 format (no "INCIDENT" header).
- Rationale: backward compatibility.

**`TestLogNotifier_IncidentWithDepChain`**
- Incident with DepChain=["C", "B", "A"].
- Expected: output contains "C → B → A" or equivalent chain rendering.
- Rationale: dependency chain is visible.

### 7.2 `slack_test.go` — Incident rendering (append 3 cases)

**`TestSlackNotifier_MultiServiceIncidentBlocks`**
- Incident with 2 alerts. Format Slack message.
- Expected: header block contains "INCIDENT" and root cause service.
  At least 3 blocks (header + 2 service sections).
- Rationale: Slack renders multi-service incidents.

**`TestSlackNotifier_SingleAlertIncidentBackwardCompat`**
- Incident with 1 alert, no root service.
- Expected: Slack blocks match Phase 3 format (no incident header).
- Rationale: backward compatibility.

**`TestSlackNotifier_IncidentDepChainInHeader`**
- Incident with DepChain set.
- Expected: header block text contains the dependency chain.
- Rationale: chain shown in Slack header.

### 7.3 `dispatcher_test.go` — update existing tests

All existing dispatcher tests must be updated to wrap their `Alert` in
an `Incident` (using `WrapAlerts` or constructing single-alert Incidents).
Assertions remain the same except the `Dispatch` signature changes.

**No new test cases** — this is a mechanical update to the test harness.

---

## 8. Integration Smoke Test

**File:** `internal/correlator/integration_test.go`

**`TestCorrelator_EndToEnd_CascadingFailure`**

Setup:
- Graph: `order-svc → payment-svc → bank-gw`.
- Correlation window: 5s (short for test).
- FakeClock.

Steps:
1. Send 3 alerts within the window:
   - `bank-gw`: 0 errors, NEW pattern ("service unreachable")
   - `payment-svc`: 200 errors, SPIKE (connection refused)
   - `order-svc`: 50 errors, SPIKE (timeout calling payment)
2. Advance clock past window. Close input.
3. Drain output.

Assertions:
- Exactly 1 Incident emitted.
- `Services` contains all 3 services.
- `RootService == "bank-gw"` (depth 2, deepest).
- `DepChain == ["bank-gw", "payment-svc", "order-svc"]`.
- `len(Alerts) == 3`.

---

## 9. Benchmark

**`BenchmarkCorrelator_GroupAlerts`** in `correlator_test.go`:

```go
func BenchmarkCorrelator_GroupAlerts(b *testing.B) {
    // Graph with 50 services, 100 edges.
    // Buffer 20 alerts from 10 services.
    // Measure: ns/op for groupAlerts() (union-find + root-cause).
}
```

Target: < 100µs per flush with 20 alerts (graph traversal is O(V+E)
per Connected check, but union-find amortizes repeated queries).

---

## 10. What We Do Not Test Here

| Concern | Reason omitted |
|---|---|
| LLM diagnosis prompt assembly | Phase 5 |
| Incident lifecycle (OPEN/ONGOING/RESOLVED) | Phase 5/6 |
| Auto-discovered dependency graph | Future enhancement |
| Production-scale graphs (1000+ services) | Benchmark covers it directionally |
| Config parsing / YAML loading errors | Covered by build test + manual check |
| Concurrent correlator access | Single-goroutine by design |

---

## 11. Test Execution Checklist

```bash
# Unit tests with race detector
go test -race ./internal/correlator/... ./internal/notify/...

# Full suite regression check (Phase 1-4)
go test -race ./...

# Benchmarks
go test ./internal/correlator/ -run ^$ -bench=BenchmarkCorrelator -benchtime=5s

# Manual smoke test with multi-service fixture
go run ./cmd/agent/ config/config-file.yaml
```
