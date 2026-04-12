# Phase 6: Notification + Deduplication — Design Document

**Author**: bzhang  
**Date**: April 2026  
**Status**: Proposed  
**Depends on**: Phases 1–5 (Ingest → Filter → Pattern → Anomaly → Correlator → Diagnoser)

---

## 1. Goals

1. **Incident lifecycle management** — Track incidents through OPEN → ONGOING → RESOLVED states so users receive exactly three types of notifications: opened, updated (throttled), and resolved.
2. **Deduplication** — Suppress repeated notifications for the same ongoing incident within a configurable quiet window.
3. **Auto-resolve** — Automatically transition incidents to RESOLVED when no new anomalies arrive within a configurable timeout.
4. **Severity routing** — Route notifications to channels based on incident severity (e.g., P1 → all channels, P3 → log only).
5. **Email notifier** — New `EmailNotifier` implementation using SMTP.
6. **Teams notifier** — New `TeamsNotifier` implementation using Microsoft Teams incoming webhook with Adaptive Cards.

## 2. Non-Goals

- PagerDuty, SMS, or generic webhook notifiers (future work).
- Persistent incident store across agent restarts (in-memory is sufficient for now).
- RAG over past incidents.
- Web UI or dashboard.

---

## 3. Architecture

### 3.1 Pipeline Change

Current pipeline (Phases 1–5):
```
Ingest → Filter → Pattern → Anomaly → Correlator → Diagnoser → Dispatcher → Notifiers
```

New pipeline with Phase 6:
```
Ingest → Filter → Pattern → Anomaly → Correlator → Diagnoser → LifecycleManager → Dispatcher → Notifiers
                                                                       ▲
                                                                       │
                                                              (auto-resolve timer)
```

The **LifecycleManager** is a new pipeline stage inserted between the Diagnoser output and the Dispatcher. It:
- Reads `<-chan Incident` from upstream.
- Maintains an in-memory map of active incidents keyed by incident ID.
- Outputs `<-chan IncidentNotification` to the Dispatcher, where each notification carries the incident state and the event type (opened / updated / resolved).

### 3.2 Incident Lifecycle State Machine

```
    New incident arrives
           │
           ▼
    ┌──────────────┐
    │     OPEN     │  → Send "opened" notification
    └──────┬───────┘
           │
           │ Same incident ID arrives again
           │ (within dedup window → suppress)
           │ (after dedup window → send "updated")
           ▼
    ┌──────────────┐
    │   ONGOING    │  → Optionally send "updated" notification (throttled)
    └──────┬───────┘
           │
           │ No new events for resolve_after duration
           ▼
    ┌──────────────┐
    │   RESOLVED   │  → Send "resolved" notification, remove from map
    └──────────────┘
```

**State transitions:**

| Current State | Event | Condition | New State | Action |
|---|---|---|---|---|
| (none) | Incident arrives | ID not tracked | OPEN | Store incident; emit OPENED notification |
| OPEN | Incident arrives | Same ID, within dedup window | OPEN | Update stored incident; suppress notification |
| OPEN | Incident arrives | Same ID, past dedup window | ONGOING | Update stored incident; emit UPDATED notification; reset dedup timer |
| ONGOING | Incident arrives | Same ID, within dedup window | ONGOING | Update stored incident; suppress notification |
| ONGOING | Incident arrives | Same ID, past dedup window | ONGOING | Update stored incident; emit UPDATED notification; reset dedup timer |
| OPEN/ONGOING | No events | resolve_after elapsed since last event | RESOLVED | Emit RESOLVED notification; delete from map |

### 3.3 Data Structures

```go
// IncidentStatus represents the lifecycle state of an incident.
type IncidentStatus string

const (
    StatusOpen     IncidentStatus = "OPEN"
    StatusOngoing  IncidentStatus = "ONGOING"
    StatusResolved IncidentStatus = "RESOLVED"
)

// Added to Incident struct (see §3.6 below):
//   Status    IncidentStatus
//   EventType string         // "opened", "updated", "resolved"
//   Duration  time.Duration  // elapsed since OpenedAt (set on resolved)

// trackedIncident is the internal bookkeeping held by LifecycleManager.
type trackedIncident struct {
    incident     Incident       // latest data (alerts, diagnosis, etc.)
    status       IncidentStatus
    firstSeen    time.Time
    lastSeen     time.Time
    lastNotified time.Time      // when the last notification was emitted
    updateCount  int
}
```

> **No wrapper type.** The LifecycleManager outputs the same `<-chan Incident`
> as its input, but with `Status`, `EventType`, and `Duration` populated.
> This keeps the pipeline uniform — every stage transforms channels of the
> same or related types — and avoids a second type that duplicates Incident fields.

### 3.4 LifecycleManager

**Package**: `internal/notify`  
**File**: `lifecycle.go`

```go
type LifecycleConfig struct {
    DedupWindow   time.Duration // Minimum interval between notifications for same incident (default: 5m)
    ResolveAfter  time.Duration // Auto-resolve after no new events (default: 10m)
    CheckInterval time.Duration // How often to scan for resolvable incidents (default: 1m)
}

type LifecycleManager struct {
    cfg      LifecycleConfig
    now      func() time.Time  // injectable clock for testing (defaults to time.Now)
    mu       sync.Mutex
    tracked  map[string]*trackedIncident
}

func NewLifecycleManager(cfg LifecycleConfig) *LifecycleManager

// Run consumes incidents from the upstream channel and produces
// Incident events (with Status/EventType/Duration set) on the output
// channel. It also runs a background goroutine to check for
// auto-resolve candidates.
func (lm *LifecycleManager) Run(ctx context.Context, in <-chan Incident) <-chan Incident
```

**Key behaviors:**
- Thread-safe: the resolve-checker goroutine and the event-processing goroutine both access `tracked` under `mu`.
- When the input channel closes or ctx is cancelled, all remaining OPEN/ONGOING incidents are resolved and emitted before the output channel closes.
- The resolve-checker goroutine ticks every `CheckInterval` and emits RESOLVED notifications for any incident whose `lastSeen` is older than `ResolveAfter`.
- **Clock injection:** The `now` field defaults to `time.Now` but can be replaced in tests to control time deterministically, avoiding flaky timing-based tests.

**Incident update semantics:** When a duplicate incident ID arrives, the stored `trackedIncident` is updated as follows:
- `incident` is replaced with the latest incoming Incident (so the most recent alerts, diagnosis, and severity are used for notifications).
- `firstSeen` is preserved from the original event; `OpenedAt` on the emitted Incident is always set to `firstSeen`.
- `lastSeen` is set to `now()`.
- `updateCount` is incremented.

### 3.5 Severity Routing

The Dispatcher is enhanced to support per-channel severity filtering. Each notifier is wrapped with a severity filter:

```go
type routedNotifier struct {
    notifier   Notifier
    severities map[string]bool // e.g., {"P1": true, "P2": true}
}
```

**Updated Dispatcher:**

```go
type Dispatcher struct {
    notifiers []routedNotifier
    timeout   time.Duration
}

func NewDispatcher(routes []NotifierRoute) *Dispatcher

// NotifierRoute pairs a Notifier with the severities it should handle.
type NotifierRoute struct {
    Notifier   Notifier
    Severities []string // empty = all severities
}

// Dispatch sends the incident to all notifiers whose severity filter matches.
// It reads inc.Severity (set by diagnoser) to decide routing.
func (d *Dispatcher) Dispatch(ctx context.Context, inc Incident) error
```

**Backward compatibility:** If `Severities` is empty/nil for a route, all incidents are sent to that notifier (matching current behavior). The `Dispatch` method signature keeps `Incident` — no new wrapper types introduced.

### 3.6 Notifier Interface Update

The `Notifier` interface signature stays the same — `Send(ctx, Incident)`. The `Incident` struct gains three fields so notifiers can format messages differently for opened/updated/resolved events:

```go
// Added to Incident struct:
type Incident struct {
    // ... existing fields (ID, Services, RootService, DepChain, Alerts,
    //     OpenedAt, Window, Diagnosis, Severity, Suggestions) ...

    Status    IncidentStatus // OPEN, ONGOING, RESOLVED
    EventType string         // "opened", "updated", "resolved"
    Duration  time.Duration  // elapsed since OpenedAt (set by LifecycleManager on resolve)
}
```

This keeps the `Notifier` interface unchanged, the pipeline uniform (`<-chan Incident` at every stage boundary), and gives notifiers the lifecycle context they need to format appropriately.

---

## 4. New Notifiers

### 4.1 EmailNotifier

**File**: `internal/notify/email.go`

```go
type EmailConfig struct {
    Host       string   // SMTP host (e.g., "smtp.company.com")
    Port       int      // SMTP port (default: 587)
    Username   string   // SMTP auth username
    Password   string   // SMTP auth password
    From       string   // sender address
    Recipients []string // recipient addresses
    UseTLS     bool     // use STARTTLS (default: true)
}

type EmailNotifier struct {
    cfg EmailConfig
}

func NewEmailNotifier(cfg EmailConfig) *EmailNotifier
func (e *EmailNotifier) Name() string  // returns "email"
func (e *EmailNotifier) Send(ctx context.Context, incident Incident) error
```

**Format:**
- Subject: `[P1] INCIDENT OPENED — bank-gateway DOWN` (severity + event type + root service)
- Body: HTML formatted with incident details, diagnosis, suggestions, and affected services.
- For RESOLVED events: subject includes "RESOLVED" and body includes duration.

**Implementation details:**
- Uses Go's `net/smtp` package with STARTTLS.
- Timeout via the context passed to `Send`.
- Template-based HTML body using `html/template`.

### 4.2 TeamsNotifier

**File**: `internal/notify/teams.go`

```go
type TeamsConfig struct {
    WebhookURL string
}

type TeamsNotifier struct {
    cfg    TeamsConfig
    client *http.Client
}

func NewTeamsNotifier(cfg TeamsConfig) *TeamsNotifier
func (t *TeamsNotifier) Name() string  // returns "teams"
func (t *TeamsNotifier) Send(ctx context.Context, incident Incident) error
```

**Format:**
- Uses Microsoft Teams Incoming Webhook with Adaptive Card payload (schema 1.4).
- Card sections: header (severity badge + title), facts (root cause, affected services, duration), diagnosis block, suggestions list.
- Color-coded by severity: P1 = red (`attention`), P2 = yellow (`warning`), P3 = blue (`accent`).

**Implementation details:**
- HTTP POST to webhook URL with `Content-Type: application/json`.
- Adaptive Card JSON built using Go structs (no external dependency).
- Timeout via `http.Client` and context.

---

## 5. Configuration Changes

### 5.1 Updated `config.yaml` Schema

```yaml
notification:
  # Lifecycle management
  dedup_window: "5m"      # suppress duplicate notifications within this window
  resolve_after: "10m"    # auto-resolve if no new events for this duration
  check_interval: "1m"    # how often to scan for auto-resolve candidates

  channels:
    - type: log
      severities: [P1, P2, P3]

    - type: slack
      webhook_url: "https://hooks.slack.com/services/..."
      severities: [P1, P2, P3]

    - type: email
      smtp_host: "smtp.company.com"
      smtp_port: 587
      smtp_username: "alerts@company.com"
      smtp_password: "${SMTP_PASSWORD}"
      from: "alerts@company.com"
      recipients:
        - "oncall@company.com"
        - "sre-team@company.com"
      use_tls: true
      severities: [P1, P2]

    - type: teams
      webhook_url: "https://outlook.office.com/webhook/..."
      severities: [P1, P2, P3]
```

### 5.2 Config Struct Changes in `main.go`

```go
type NotificationConfig struct {
    DedupWindow   string          `yaml:"dedup_window"`
    ResolveAfter  string          `yaml:"resolve_after"`
    CheckInterval string          `yaml:"check_interval"`
    Channels      []ChannelConfig `yaml:"channels"`
}

type ChannelConfig struct {
    Type         string   `yaml:"type"`
    Severities   []string `yaml:"severities"`
    // Slack / Teams
    WebhookURL   string   `yaml:"webhook_url"`
    // Email
    SMTPHost     string   `yaml:"smtp_host"`
    SMTPPort     int      `yaml:"smtp_port"`
    SMTPUsername  string   `yaml:"smtp_username"`
    SMTPPassword string   `yaml:"smtp_password"`
    From         string   `yaml:"from"`
    Recipients   []string `yaml:"recipients"`
    UseTLS       *bool    `yaml:"use_tls"`
}
```

---

## 6. Pipeline Wiring Changes (`main.go`)

The `run()` function changes to insert the LifecycleManager:

```go
// After diagnoser stage (or correlator if diagnoser disabled):
lifecycleCfg := notify.LifecycleConfig{
    DedupWindow:   parseDuration(cfg.Notification.DedupWindow, 5*time.Minute),
    ResolveAfter:  parseDuration(cfg.Notification.ResolveAfter, 10*time.Minute),
    CheckInterval: parseDuration(cfg.Notification.CheckInterval, 1*time.Minute),
}
lm := notify.NewLifecycleManager(lifecycleCfg)
managed := lm.Run(ctx, diagnosed)

// Dispatch loop (same Incident type, now with Status/EventType/Duration set):
for inc := range managed {
    if err := dispatcher.Dispatch(ctx, inc); err != nil {
        slog.Error("dispatch failed", "err", err, "event", inc.EventType, "id", inc.ID)
    }
}
```

The `buildNotifiers` function now returns `[]NotifierRoute` and handles the new channel types:

```go
func buildNotifiers(cfg NotificationConfig) []notify.NotifierRoute {
    var routes []notify.NotifierRoute
    for _, ch := range cfg.Channels {
        var n notify.Notifier
        switch ch.Type {
        case "slack":
            n = notify.NewSlackNotifier(ch.WebhookURL)
        case "log":
            n = notify.NewLogNotifier(nil)
        case "email":
            n = notify.NewEmailNotifier(notify.EmailConfig{...})
        case "teams":
            n = notify.NewTeamsNotifier(notify.TeamsConfig{...})
        default:
            slog.Warn("unknown notifier type", "type", ch.Type)
            continue
        }
        routes = append(routes, notify.NotifierRoute{
            Notifier:   n,
            Severities: ch.Severities,
        })
    }
    return routes
}
```

---

## 7. Files Changed / Created

| File | Action | Description |
|---|---|---|
| `internal/notify/lifecycle.go` | **Create** | LifecycleManager: state machine, dedup, auto-resolve |
| `internal/notify/email.go` | **Create** | EmailNotifier: SMTP-based email delivery |
| `internal/notify/teams.go` | **Create** | TeamsNotifier: MS Teams Adaptive Card webhook |
| `internal/notify/notifier.go` | **Modify** | Add `IncidentStatus`, `NotifierRoute`, `routedNotifier`; update Dispatcher to accept routes + filter by severity |
| `internal/notify/incident.go` | **Modify** | Add `Status`, `EventType`, `Duration` fields to Incident |
| `internal/notify/slack.go` | **Modify** | Handle event types (opened/updated/resolved) in formatting |
| `internal/notify/log.go` | **Modify** | Handle event types in log output |
| `cmd/agent/main.go` | **Modify** | Wire LifecycleManager, update buildNotifiers, new config fields |

---

## 8. Error Handling

- **SMTP failure**: `EmailNotifier.Send` returns an error; Dispatcher logs it and continues to other notifiers (existing fan-out behavior).
- **Teams webhook failure**: Same — HTTP errors are returned, Dispatcher logs and continues.
- **Lifecycle manager shutdown**: On ctx cancellation, all tracked incidents are resolved before the output channel closes, ensuring "resolved" notifications are sent during graceful shutdown.
- **Concurrent access**: `LifecycleManager.tracked` is protected by `sync.Mutex`. The resolve-checker goroutine and the main processing goroutine serialize access.

---

## 9. Observability

- Log lifecycle transitions: `slog.Info("incident state change", "id", id, "from", old, "to", new, "event", eventType)`.
- Log suppressed notifications: `slog.Debug("notification suppressed (dedup)", "id", id, "within", dedupWindow)`.
- Log auto-resolve: `slog.Info("incident auto-resolved", "id", id, "duration", duration)`.

---

## 10. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| In-memory state lost on restart | Active incidents "forgotten", may re-open as new | Acceptable for v1; add persistent store later |
| SMTP blocks for a long time | Delays notifications for other incidents | Context-based timeout (30s default); Dispatcher sends to notifiers concurrently |
| Teams webhook rate limits | 429 responses | Retry with backoff up to 3 attempts within Send() |
| Incident ID collision (different incidents, same ID) | Wrong dedup | SHA256 of sorted services + time truncation makes this extremely unlikely |
| Resolve-checker goroutine leak | Memory/resource leak | Tied to ctx cancellation; tested with context timeout |
