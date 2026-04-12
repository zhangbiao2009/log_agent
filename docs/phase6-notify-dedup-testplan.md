# Phase 6: Notification + Deduplication — Test Plan

**Author**: bzhang  
**Date**: April 2026  
**Companion**: [phase6-notify-dedup-design.md](phase6-notify-dedup-design.md)

---

## 1. Test Strategy

### 1.1 Principles

- **No external dependencies in unit tests.** SMTP, Teams webhooks, and HTTP are tested via interfaces and mocks — never real network calls.
- **Table-driven tests** for all logic with deterministic inputs.
- **Race detector** (`go test -race`) on every test file.
- **Context-based cancellation** tested explicitly to verify graceful shutdown.
- **Time control** via injectable `now()` function on LifecycleManager for dedup-window checks, combined with short real durations (50–200ms) for auto-resolve ticker tests. This avoids both flaky tests and the complexity of a full fake-clock.

### 1.2 Test Organization

| File | Tests | Package |
|---|---|---|
| `lifecycle_test.go` | LifecycleManager state machine, dedup, auto-resolve, shutdown | `notify` |
| `email_test.go` | EmailNotifier formatting + SMTP interaction | `notify` |
| `teams_test.go` | TeamsNotifier formatting + HTTP payload | `notify` |
| `dispatcher_test.go` | Severity routing (extend existing) | `notify` |
| `slack_test.go` | Event-type-aware formatting (extend existing) | `notify` |
| `log_test.go` | Event-type-aware log output (extend existing) | `notify` |

---

## 2. LifecycleManager Tests (`lifecycle_test.go`)

The LifecycleManager is the core of Phase 6. Its tests must cover every state transition, dedup suppression, auto-resolve, and shutdown behavior.

### 2.1 State Machine Tests

| # | Test Name | Description | Input | Expected Output |
|---|---|---|---|---|
| 1 | `TestLifecycle_NewIncident_EmitsOpened` | First time an incident ID is seen → OPEN + "opened" event | 1 incident | 1 notification: status=OPEN, eventType="opened" |
| 2 | `TestLifecycle_DuplicateWithinWindow_Suppressed` | Same ID arrives within dedup window → no new notification | 2 incidents (same ID, 1s apart), dedup=5m | 1 notification (the first "opened") |
| 3 | `TestLifecycle_DuplicateAfterWindow_EmitsUpdated` | Same ID after dedup window → ONGOING + "updated" event | 2 incidents (same ID, 6m apart), dedup=5m | 2 notifications: "opened" then "updated" |
| 4 | `TestLifecycle_MultipleUpdates_Throttled` | Rapid updates after first dedup window → only one "updated" per window | 5 incidents (same ID, at t=0,1m,6m,7m,12m), dedup=5m | 3 notifications: "opened"(t=0), "updated"(t=6m), "updated"(t=12m) |
| 5 | `TestLifecycle_DifferentIDs_Independent` | Two different incident IDs tracked independently | 2 incidents with different IDs | 2 "opened" notifications |
| 6 | `TestLifecycle_UpdatedIncident_HasLatestData` | When an "updated" notification fires, it carries the latest Incident data (e.g., updated alerts, diagnosis) | 2 incidents same ID with different alerts | "updated" notification has latest alerts (replaced, not merged) |

### 2.2 Auto-Resolve Tests

| # | Test Name | Description | Input | Expected Output |
|---|---|---|---|---|
| 7 | `TestLifecycle_AutoResolve_AfterTimeout` | No new events → incident auto-resolves | 1 incident, resolve_after=100ms, check_interval=50ms | 2 notifications: "opened" then "resolved" |
| 8 | `TestLifecycle_AutoResolve_ResetByNewEvent` | New event resets the auto-resolve timer | incident at t=0, another at t=80ms, resolve_after=100ms | No resolve at t=100ms; resolve eventually at t=180ms+ |
| 9 | `TestLifecycle_Resolved_IncidentHasDuration` | Resolved notification has Duration field set | 1 incident, auto-resolve | Duration ≈ resolve_after |
| 10 | `TestLifecycle_Resolved_RemovedFromTracking` | After resolving, same ID arriving again is treated as new OPEN | incident → auto-resolve → same ID again | 3 notifications: "opened", "resolved", "opened" |

### 2.3 Shutdown Tests

| # | Test Name | Description | Input | Expected Output |
|---|---|---|---|---|
| 11 | `TestLifecycle_ContextCancel_ResolvesAll` | ctx cancellation resolves all tracked incidents | 3 open incidents, cancel ctx | 3 "opened" + 3 "resolved" notifications |
| 12 | `TestLifecycle_InputCloses_ResolvesAll` | Upstream channel closes → resolve all, close output | 2 open incidents, close input chan | 2 "opened" + 2 "resolved" notifications |
| 13 | `TestLifecycle_EmptyInput_ClosesOutput` | No incidents received, input closes → output closes cleanly | Empty input channel | Output channel closes, no notifications |

### 2.4 Concurrency Tests

| # | Test Name | Description |
|---|---|---|
| 14 | `TestLifecycle_ConcurrentResolveAndEvent` | Auto-resolve fires at the same instant a new event arrives for same ID. Race detector must pass. |
| 15 | `TestLifecycle_RaceDetector` | Send 100 incidents (20 unique IDs, 5 each) with short dedup/resolve windows. Verify no race with `-race`. |

---

## 3. Dispatcher / Severity Routing Tests (`dispatcher_test.go`)

Extend the existing dispatcher tests with severity routing.

| # | Test Name | Description | Expected |
|---|---|---|---|
| 16 | `TestDispatch_SeverityRouting_P1MatchesAll` | P1 incident, 3 notifiers: one accepts [P1], one [P1,P2], one [P3] | First two receive it, third does not |
| 17 | `TestDispatch_SeverityRouting_EmptySeverities_MatchesAll` | Notifier with no severity filter | Receives all incidents regardless of severity |
| 18 | `TestDispatch_SeverityRouting_NoMatch` | P3 incident, all notifiers only accept [P1] | No notifiers called |
| 19 | `TestDispatch_SeverityRouting_NoSeverityOnIncident` | Incident with empty severity string, notifier accepts [P1] | Notifier is NOT called (empty severity does not match) |
| 20 | `TestDispatch_EventTypePassedThrough` | Verify notifiers receive the Incident with Status and EventType set | Notifier sees inc.Status and inc.EventType |

---

## 4. EmailNotifier Tests (`email_test.go`)

### 4.1 Approach

Inject a mock SMTP `sendMail` function to capture the message without real network I/O:

```go
type sendMailFunc func(addr string, auth smtp.Auth, from string, to []string, msg []byte) error
```

The `EmailNotifier` accepts this function, defaulting to `smtp.SendMail` in production.

| # | Test Name | Description | Validation |
|---|---|---|---|
| 21 | `TestEmail_OpenedIncident_SubjectAndBody` | Format for "opened" event | Subject contains `[P1] INCIDENT OPENED`, body has diagnosis + suggestions in HTML |
| 22 | `TestEmail_ResolvedIncident_SubjectAndBody` | Format for "resolved" event | Subject contains `RESOLVED`, body has duration |
| 23 | `TestEmail_UpdatedIncident_SubjectAndBody` | Format for "updated" event | Subject contains `UPDATE` |
| 24 | `TestEmail_SingleAlert_Format` | Single-alert incident (no correlation) | Subject has service name, body has alert details |
| 25 | `TestEmail_HTMLEscaping` | Incident fields with `<script>` injection | HTML body output is properly escaped |
| 26 | `TestEmail_SendError_Propagated` | Mock returns error | `Send()` returns the error |
| 27 | `TestEmail_MultipleRecipients` | 3 recipients configured | `sendMail` called with all 3 in `to` |
| 28 | `TestEmail_Name` | `Name()` returns "email" | — |

### 4.2 Template Tests

| # | Test Name | Description |
|---|---|---|
| 29 | `TestEmail_TemplateRenders_WithAllFields` | Full incident with all fields populated renders without template error |
| 30 | `TestEmail_TemplateRenders_MinimalFields` | Incident with only required fields (no diagnosis, no suggestions) renders cleanly |

---

## 5. TeamsNotifier Tests (`teams_test.go`)

### 5.1 Approach

Use `httptest.NewServer` to capture the POST payload.

| # | Test Name | Description | Validation |
|---|---|---|---|
| 31 | `TestTeams_OpenedIncident_AdaptiveCard` | "opened" event produces valid Adaptive Card JSON | JSON has `type: "AdaptiveCard"`, header with severity, facts with services |
| 32 | `TestTeams_ResolvedIncident_AdaptiveCard` | "resolved" event card includes duration | Card header says "RESOLVED", duration fact present |
| 33 | `TestTeams_UpdatedIncident_AdaptiveCard` | "updated" event card | Card header says "UPDATE" |
| 34 | `TestTeams_SeverityColor_P1Red` | P1 → attention style | Card uses `attention` color |
| 35 | `TestTeams_SeverityColor_P2Yellow` | P2 → warning style | Card uses `warning` color |
| 36 | `TestTeams_SeverityColor_P3Blue` | P3 → accent style | Card uses `accent` color |
| 37 | `TestTeams_HTTPError_Returned` | Webhook returns 500 | `Send()` returns error |
| 38 | `TestTeams_ContextCancelled` | ctx cancelled before POST | `Send()` returns context error |
| 39 | `TestTeams_Name` | `Name()` returns "teams" | — |
| 40 | `TestTeams_DiagnosisBlock` | Incident with diagnosis + suggestions | Card has diagnosis section and numbered suggestion list |

---

## 6. Existing Notifier Updates

### 6.1 SlackNotifier (`slack_test.go` — extend)

| # | Test Name | Description |
|---|---|---|
| 41 | `TestSlack_OpenedEvent_Format` | "opened" event includes "🔴 INCIDENT OPENED" header |
| 42 | `TestSlack_ResolvedEvent_Format` | "resolved" event includes "✅ INCIDENT RESOLVED" + duration |
| 43 | `TestSlack_UpdatedEvent_Format` | "updated" event includes "🔄 INCIDENT UPDATE" |

### 6.2 LogNotifier (`log_test.go` — extend)

| # | Test Name | Description |
|---|---|---|
| 44 | `TestLog_OpenedEvent_Output` | Log line includes `event=opened status=OPEN` |
| 45 | `TestLog_ResolvedEvent_Output` | Log line includes `event=resolved duration=...` |
| 46 | `TestLog_UpdatedEvent_Output` | Log line includes `event=updated status=ONGOING` |

---

## 7. Integration / Pipeline Tests

| # | Test Name | File | Description |
|---|---|---|---|
| 47 | `TestPipeline_LifecycleToDispatcher` | `lifecycle_test.go` | Wire LifecycleManager → Dispatcher with mock notifiers. Send 3 incidents (2 same ID, 1 different). Verify correct notifications reach notifiers with correct event types. |
| 48 | `TestPipeline_SeverityRouting_EndToEnd` | `dispatcher_test.go` | P1 incident through lifecycle → dispatcher with log (all) + mock (P1 only). Verify both receive "opened". |

---

## 8. Edge Cases

| # | Test Name | File | Description |
|---|---|---|---|
| 49 | `TestLifecycle_ZeroDedupWindow` | `lifecycle_test.go` | DedupWindow=0 means every duplicate emits "updated" |
| 50 | `TestLifecycle_VeryShortResolve` | `lifecycle_test.go` | ResolveAfter=1ms, verify rapid resolve without panic |
| 51 | `TestEmail_EmptyRecipients` | `email_test.go` | No recipients configured → Send returns error |
| 52 | `TestTeams_EmptyWebhookURL` | `teams_test.go` | Empty URL → Send returns error |

---

## 9. Test Execution

```bash
# Run all Phase 6 tests with race detector
cd log_agent
go test -race -v ./internal/notify/... -run "Lifecycle|Email_|Teams_|Dispatch_Severity|Pipeline_"

# Run just lifecycle tests
go test -race -v ./internal/notify/ -run TestLifecycle

# Run with coverage
go test -race -coverprofile=coverage.out ./internal/notify/...
go tool cover -func=coverage.out | grep -E "lifecycle|email|teams|dispatch"
```

---

## 10. Coverage Targets

| Component | Target | Rationale |
|---|---|---|
| `lifecycle.go` | ≥ 90% | Core state machine — all transitions must be covered |
| `email.go` | ≥ 85% | Template rendering + error paths |
| `teams.go` | ≥ 85% | JSON payload construction + error paths |
| Dispatcher (severity routing) | ≥ 90% | Routing logic must be correct |
| Slack/Log event-type handling | ≥ 80% | Extension of existing well-tested code |

---

## 11. Test Count Summary

| File | Tests |
|---|---|
| `lifecycle_test.go` | 18 (state machine: 6, auto-resolve: 4, shutdown: 3, concurrency: 2, pipeline: 1, edge: 2) |
| `dispatcher_test.go` | 6 (severity routing: 5, pipeline: 1) |
| `email_test.go` | 11 (format: 4, security: 1, error: 1, recipients: 1, name: 1, template: 2, edge: 1) |
| `teams_test.go` | 11 (format: 4, color: 3, error: 1, context: 1, name: 1, edge: 1) |
| `slack_test.go` | 3 (event formatting) |
| `log_test.go` | 3 (event formatting) |
| **Total** | **52** |
