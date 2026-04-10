# Log Analysis Agent — Design Document

**Project**: Intelligent Log Monitor for Microservices  
**Author**: bzhang  
**Date**: April 9, 2026  
**Status**: Draft  

---

## 1. Problem Statement

Our company runs a microservice architecture. When things go wrong, engineers
manually dig through logs from multiple services, try to figure out which
service is the root cause, and guess at fixes. This is slow, error-prone,
and doesn't scale.

**Key pain points:**
- No centralized alerting configured for many services
- Errors cascade across services — hard to find the root cause
- Same types of incidents repeat, but lessons aren't reused
- On-call engineers waste time reading thousands of duplicate log lines

## 2. What This Agent Does

A Go program that continuously monitors logs from all services, detects
anomalies, correlates errors across services, and uses an LLM to diagnose
root causes and suggest fixes.

**In one sentence:** "Service B's database is down (deployed v2.3.1 five
minutes ago), causing timeouts in Service A and Service C. Recommend:
rollback Service B to v2.3.0."

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Log Sources                             │
│   Loki API  /  Kafka topic  /  file tail  (one per service)  │
└──────────────────────┬──────────────────────────────────────┘
                       │
               ┌───────▼────────┐
               │  L1: Ingest +   │  Go: keyword filter (ERROR/FATAL/WARN)
               │  Fast Filter    │  Drops ~90% of log volume
               └───────┬────────┘
                       │
               ┌───────▼────────┐
               │  L2: Pattern    │  Go: Drain algorithm
               │  Fingerprint    │  Groups logs into templates
               └───────┬────────┘  "connection timeout to <*>:<*>" (500x)
                       │
               ┌───────▼────────┐
               │  L3: Anomaly    │  Go: per-pattern rolling stats
               │  Detection      │  Triggers on spike / new pattern /
               └───────┬────────┘  error rate jump
                       │
               ┌───────▼────────┐
               │  L4: Cross-Svc  │  Go: group co-occurring anomalies
               │  Correlator     │  into a single "incident" using
               └───────┬────────┘  time window + dependency graph
                       │
               ┌───────▼────────┐
               │  L5: LLM        │  Send incident context to LLM:
               │  Diagnosis       │  log samples + dependency chain +
               └───────┬────────┘  recent deploys + past incidents (RAG)
                       │
               ┌───────▼────────┐
               │  L6: Notify     │  Slack / PagerDuty / webhook
               │  + Dedup        │  Incident lifecycle management
               └─────────────────┘
```

## 4. Layer-by-Layer Design

### L1: Ingest + Fast Filter

**Purpose:** Connect to log sources and drop non-error logs immediately.

**Input:** Raw log streams from all services.  
**Output:** Only ERROR / FATAL / WARN log lines (plus a few lines of context).

**Design decisions:**
- Support multiple log sources via an interface:
  ```go
  type LogSource interface {
      // Stream returns a channel of log lines from the given service.
      Stream(ctx context.Context, service string) (<-chan LogLine, error)
  }
  ```
- Implementations: `LokiSource` (query Loki HTTP API), `FileSource` (tail files),
  `KafkaSource` (consume from topic). Start with Loki since we already use it.
- Fast filter is just string matching: skip lines that don't contain
  `ERROR`, `FATAL`, `WARN`, or `panic`.
- Each `LogLine` carries metadata:
  ```go
  type LogLine struct {
      Service   string
      Timestamp time.Time
      Level     string    // ERROR, FATAL, WARN
      Raw       string    // original log text
  }
  ```

### L2: Pattern Fingerprint (Drain Algorithm)

**Purpose:** Group identical log patterns together so we count *kinds* of
errors, not individual lines.

**Input:** Filtered log lines from L1.  
**Output:** Pattern objects with counts.

**Why not regex?** Hand-written regex is fragile and breaks as services evolve.
Drain auto-discovers templates from the log stream.

**How Drain works:**
1. Tokenize the log by spaces.
2. Navigate a fixed-depth tree: level 1 = token count, level 2 = first token,
   level 3 = second token.
3. At the leaf, compare against existing templates by token-level similarity.
4. If similarity ≥ threshold (0.5), merge (replace differing tokens with `<*>`).
5. If no match, create a new template.

**Pre-processing:** Before Drain, apply regex to replace obvious variables:
- IP addresses → `<IP>`
- UUIDs → `<UUID>`
- Numbers → `<NUM>`
- File paths with IDs → normalize the ID parts

**Data structure:**
```go
type LogPattern struct {
    Signature    string    // templatized log, e.g. "connection timeout to <*>:<*>"
    Service      string
    Level        string
    FirstSeen    time.Time
    LastSeen     time.Time
    CountMinute  int       // count in current 1-min window
    Count5Min    int       // count in current 5-min window
    SampleLines  []string  // keep last 5 raw log lines as examples
}
```

**Drain parameters:**
| Parameter | Value | Rationale |
|---|---|---|
| `depth` | 4 | Good balance of speed vs. accuracy |
| `similarityThreshold` | 0.5 | Standard default, tune per service if needed |
| `maxChildren` | 100 | Prevent memory blowup from high-cardinality tokens |

### L3: Anomaly Detection

**Purpose:** Decide which patterns are "abnormal" and worth escalating.

**Input:** Pattern objects with counts from L2.  
**Output:** Anomaly events.

**Three trigger conditions:**

| Trigger | Logic | Why It Matters |
|---|---|---|
| **Spike** | Current count > mean + 3σ (rolling baseline) | A known error suddenly happens 100x more |
| **New pattern** | Template never seen in the last 24h | New bug or new failure mode — often the most dangerous |
| **Error rate jump** | Service-level error rate crosses threshold | Even if individual patterns are low, the aggregate is bad |

**Rolling baseline:**
- Maintain per-pattern stats: mean and standard deviation over a 24-hour
  sliding window, bucketed into 1-minute intervals.
- Store in an embedded DB (BadgerDB or SQLite) so baselines survive restarts.
- Time-of-day awareness: keep separate baselines for each hour of the day
  (traffic at 3am ≠ traffic at 3pm).

```go
type PatternStats struct {
    PatternID   string
    HourOfDay   int       // 0-23, separate baseline per hour
    Buckets     []int     // last 60 one-minute counts (circular buffer)
    Mean        float64
    StdDev      float64
}

func (s *PatternStats) IsSpike(currentCount int) bool {
    return float64(currentCount) > s.Mean + 3*s.StdDev
}
```

### L4: Cross-Service Correlator

**Purpose:** Group anomalies from multiple services that are part of the
same incident. This is what turns isolated alerts into root-cause analysis.

**Input:** Anomaly events from L3.  
**Output:** Incident objects containing correlated anomalies.

**How it works:**
1. **Time window:** Anomalies from different services that fire within a
   2-minute window are candidates for correlation.
2. **Dependency graph lookup:** Check if the affected services are connected
   in the dependency graph.
3. **Grouping:** If Service A depends on Service B and both have anomalies
   in the same window, group them into one incident.

**Dependency graph:**
- Start with a static YAML config file (see below).
- Later: auto-discover from OpenTelemetry traces.

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

**Incident structure:**
```go
type Incident struct {
    ID           string
    Status       string           // OPEN, ONGOING, RESOLVED
    Severity     string           // P1, P2, P3 (assigned by LLM)
    OpenedAt     time.Time
    LastUpdated  time.Time
    Services     []string         // all affected services
    RootService  string           // suspected root cause (deepest in dependency chain)
    Anomalies    []AnomalyEvent   // all correlated anomalies
    Diagnosis    string           // LLM output
    Suggestions  []string         // LLM fix suggestions
}
```

**Root cause heuristic:** Among correlated services, the one **deepest** in
the dependency chain (fewest dependents) is likely the root cause. Errors
cascade *upstream*: if B depends on C and both error, C is more likely the
root cause.

### L5: LLM Diagnosis

**Purpose:** Given an incident with log samples from multiple services,
produce a human-readable diagnosis and actionable fix suggestions.

**Input:** Incident object from L4.  
**Output:** Diagnosis text + severity + fix suggestions.

**Prompt structure:**
```
You are an SRE assistant diagnosing a production incident.

INCIDENT CONTEXT:
- Time: 2026-04-09 14:32 - 14:35 UTC
- Affected services: order-service, payment-service, bank-gateway
- Dependency chain: order-service → payment-service → bank-gateway
- Suspected root cause: bank-gateway (deepest in chain)

RECENT DEPLOYMENTS:
- bank-gateway v2.3.1 deployed at 14:30 (2 minutes before incident)

LOG PATTERNS (per service):

[bank-gateway] — 0 logs arriving (service appears DOWN)

[payment-service] — 200 errors/min (baseline: 0)
  Pattern: "connection refused to <*>:443" (200x)
  Samples:
    "connection refused to bank-gw-prod-1:443, request_id=abc123"
    "connection refused to bank-gw-prod-2:443, request_id=def456"

[order-service] — 50 errors/min (baseline: 2)
  Pattern: "timeout calling payment-service: context deadline exceeded" (50x)
  Samples:
    "timeout calling payment-service: context deadline exceeded after 5s"

SIMILAR PAST INCIDENTS:
- 2025-09-08: Database migration ran against production, caused 4h outage.
  Root cause was misconfigured $DB_HOST environment variable.

Based on the above, provide:
1. ROOT CAUSE: What is actually broken and why?
2. SEVERITY: P1 (customer-facing outage), P2 (degraded), or P3 (minor)
3. IMMEDIATE ACTION: What should the on-call engineer do right now?
4. FOLLOW-UP: What should be done after the incident is resolved?
```

**Context enrichment sources:**

| Source | How to Get It | Value |
|---|---|---|
| Dependency graph | Static YAML config | "A calls B calls C" — root cause reasoning |
| Recent deploys | Query deploy API / CD tool | Most outages happen right after deploys |
| Log samples | Stored in L2 (SampleLines) | Concrete evidence for the LLM |
| Past incidents | RAG over incident post-mortems | "This looks like last month's outage" |

**RAG for past incidents:**
- Maintain a knowledge base of past incident post-mortems (markdown files).
- Use the same RAG pipeline from research_agent (embed + store in vector DB).
- When a new incident fires, search for similar past incidents and include
  the top 2-3 in the prompt.

**LLM choice:** DeepSeek via litellm (same as other agents). Use
`temperature=0` for deterministic diagnosis.

**Cost control:** The entire funnel (L1-L4) exists to ensure we only call
the LLM for genuine incidents. Expected: ~5-20 LLM calls per day, not per
minute.

### L6: Notification + Deduplication

**Purpose:** Deliver the diagnosis to the right people, without alert fatigue.

**Incident lifecycle:**
```
    NEW anomaly detected
           │
           ▼
    ┌──────────┐     Same pattern within 5 min?
    │   OPEN   │◄────── No ──── Create new incident
    └────┬─────┘
         │
         │ More anomalies for same services?
         ▼
    ┌──────────┐
    │ ONGOING  │     Suppress duplicate notifications.
    └────┬─────┘     Update incident with new data.
         │
         │ No new anomalies for 10 min?
         ▼
    ┌──────────┐
    │ RESOLVED │     Send "incident resolved" message.
    └──────────┘
```

**Notification channels are pluggable via a `Notifier` interface:**

```go
// Notifier is the interface all notification channels implement.
// Adding a new channel (Teams, email, SMS, etc.) means implementing
// this interface — no changes to the rest of the pipeline.
type Notifier interface {
    // Send delivers a notification for the given incident.
    // The implementation formats the message for its channel.
    Send(ctx context.Context, incident Incident) error

    // Name returns the channel name (for logging/config).
    Name() string
}
```

**Built-in implementations:**

| Implementation | Channel | Use Case |
|---|---|---|
| `SlackNotifier` | Slack webhook | Team channels, default |
| `TeamsNotifier` | Microsoft Teams webhook | Teams-based orgs |
| `EmailNotifier` | SMTP / SendGrid | Stakeholder summaries |
| `SMSNotifier` | Twilio / SNS | P1 on-call escalation |
| `PagerDutyNotifier` | PagerDuty Events API | On-call paging |
| `WebhookNotifier` | Generic HTTP POST | Custom integrations |
| `LogNotifier` | Stdout/file | Local dev / testing |

Multiple notifiers can be active simultaneously. Severity routing
determines which channels fire for each severity level:

**Severity routing (configured in `config.yaml`):**

```yaml
notification:
  channels:
    - type: slack
      webhook_url: "https://hooks.slack.com/..."
      severities: [P1, P2, P3]          # all incidents
      channel_map:
        P1: "#incidents"
        P2: "#incidents"
        P3: "#service-health"
    - type: pagerduty
      routing_key: "..."
      severities: [P1]                   # only pages for P1
    - type: email
      smtp_host: "smtp.company.com"
      recipients: ["oncall@company.com"]
      severities: [P1, P2]              # email for P1+P2
    - type: teams
      webhook_url: "https://outlook.office.com/webhook/..."
      severities: [P1, P2, P3]
```

**Notification format example:**
```
🔴 P1 INCIDENT — bank-gateway DOWN

Root cause: bank-gateway stopped responding after v2.3.1 deploy
  at 14:30. payment-service and order-service are cascading.

Immediate action: Rollback bank-gateway to v2.3.0

Affected: bank-gateway, payment-service, order-service
Duration: 3 min (ongoing)
Similar past incident: Q3 2025 outage (config error after deploy)
```

Each `Notifier` implementation adapts this content to its channel's
format (Slack blocks, Teams adaptive cards, HTML email, plain text SMS).

## 5. Project Structure

```
log_agent/
├── DESIGN.md               ← this file
├── config/
│   ├── services.yaml       ← service dependency graph
│   └── config.yaml         ← thresholds, Loki URL, notification channels, etc.
├── cmd/
│   └── agent/
│       └── main.go         ← entry point
├── internal/
│   ├── ingest/
│   │   ├── source.go       ← LogSource interface
│   │   ├── loki.go         ← Loki API implementation
│   │   └── filter.go       ← fast ERROR/FATAL/WARN filter
│   ├── drain/
│   │   ├── drain.go        ← Drain algorithm implementation
│   │   ├── tree.go         ← prefix tree data structure
│   │   └── preprocess.go   ← regex normalization (IPs, UUIDs, etc.)
│   ├── anomaly/
│   │   ├── detector.go     ← spike / new-pattern / rate-jump detection
│   │   └── stats.go        ← rolling baseline (mean, stddev)
│   ├── correlator/
│   │   ├── correlator.go   ← cross-service incident grouping
│   │   └── depgraph.go     ← load + query dependency graph
│   ├── diagnosis/
│   │   ├── llm.go          ← LLM prompt assembly + call
│   │   └── rag.go          ← search past incidents for context
│   ├── notify/
│   │   ├── notifier.go     ← Notifier interface + dispatcher (fan-out to channels)
│   │   ├── slack.go        ← Slack webhook implementation
│   │   ├── teams.go        ← Microsoft Teams webhook implementation
│   │   ├── email.go        ← SMTP / SendGrid implementation
│   │   ├── sms.go          ← Twilio / SNS implementation
│   │   ├── pagerduty.go    ← PagerDuty Events API implementation
│   │   ├── webhook.go      ← Generic HTTP POST (custom integrations)
│   │   └── dedup.go        ← incident lifecycle (open/ongoing/resolved)
│   └── store/
│       └── store.go        ← persistence for baselines + incidents (SQLite/BadgerDB)
├── incidents/               ← past incident post-mortems (for RAG)
│   └── 2025-09-08-q3-outage.md
└── go.mod
```

## 6. Phased Roadmap

### Phase 1: Error Catcher (Week 1-2)

**Goal:** Get value immediately — a program that tails logs and sends error
notifications to any configured channel.

**Build:**
- `internal/ingest/` — connect to Loki, stream ERROR/FATAL logs
- `internal/notify/notifier.go` — `Notifier` interface + dispatcher
- `internal/notify/slack.go` — first channel implementation (Slack)
- `cmd/agent/main.go` — wire them together

**Output:** "Service X produced 50 error logs in the last minute."

**Value:** Even without AI, engineers get notified of problems faster.
Adding new channels (Teams, email, SMS) is just implementing the
`Notifier` interface — no pipeline changes needed.

### Phase 2: Pattern Grouping (Week 3-4)

**Goal:** Stop Slack spam. Group identical errors into patterns.

**Build:**
- `internal/drain/` — implement Drain algorithm
- Integrate into pipeline between ingest and notify

**Output:** "Service X has 3 error patterns: DB timeout (500x), parse
error (1x), null pointer (1x)."

**Value:** Clean, deduplicated alerts. Engineers immediately see *what kinds*
of errors are happening.

### Phase 3: Anomaly Detection (Week 5-6)

**Goal:** Only alert on *meaningful* changes, not constant background noise.

**Build:**
- `internal/anomaly/` — rolling baselines + spike detection
- `internal/store/` — persist baselines across restarts

**Output:** Only fires when something *changes*: new error type, spike in
existing error, or overall error rate jump.

**Value:** Massive reduction in alert noise.

### Phase 4: LLM Diagnosis (Week 7-8)

**Goal:** Tell engineers *what's wrong and how to fix it*, not just *what happened*.

**Build:**
- `internal/diagnosis/llm.go` — prompt assembly + LLM call
- Integrate recent deploy info into prompt

**Output:** "The DB timeout spike started 2 minutes after deploying
payment-service v1.5. The new version likely has a connection pool
misconfiguration. Recommend: rollback to v1.4."

**Value:** Reduces mean-time-to-diagnosis from hours to minutes.

### Phase 5: Cross-Service Correlation (Week 9-10)

**Goal:** Stop treating cascading failures as separate incidents.

**Build:**
- `internal/correlator/` — time-window grouping + dependency graph
- `config/services.yaml` — dependency config

**Output:** "Root cause is bank-gateway (just deployed). payment-service
and order-service are cascading failures."

**Value:** Engineers fix the root cause instead of chasing symptoms.

### Phase 6: RAG Over Past Incidents (Week 11-12)

**Goal:** Learn from history. "This looks like the outage from last month."

**Build:**
- `internal/diagnosis/rag.go` — embed + search incident post-mortems
- `incidents/` directory — historical post-mortem collection

**Output:** LLM prompt includes: "Similar past incident: Q3 2025 outage 
was caused by misconfigured DB_HOST after deploy. Fix was to rollback + 
fix env vars."

**Value:** Institutional knowledge is automatically surfaced during incidents.

## 7. Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Language | Go | Same as our services. Fast, low memory, good concurrency. |
| Log parsing | Drain algorithm | Industry standard, no training needed, handles evolving log formats. |
| Anomaly detection | Rolling mean + 3σ | Simple, interpretable, per-pattern baselines. |
| LLM | DeepSeek via litellm | Cheap, good at structured reasoning. |
| Dependency graph | Static YAML (phase 1), traces later | Get value immediately, improve accuracy later. |
| Persistence | SQLite or BadgerDB | Lightweight embedded store, no external infra needed. |
| Notification | Pluggable `Notifier` interface | Slack first, add Teams/email/SMS/PagerDuty by implementing the interface. |

## 8. Cost Estimate

| Component | Cost |
|---|---|
| Log processing (L1-L4) | $0 — runs on one small VM/pod |
| LLM calls (L5) | ~$0.50-2/day — only called for genuine incidents (~5-20/day, small prompts) |
| Storage (baselines) | Negligible — SQLite on local disk |

The entire funnel exists to ensure L5 (the expensive LLM call) is invoked
as rarely as possible.

## 9. Future Enhancements

- **Auto-remediation:** For known incident types, automatically trigger
  runbooks (e.g., rollback, restart, scale up).
- **Trace integration:** Auto-discover dependency graph from OpenTelemetry.
- **Dashboard:** Web UI showing live patterns, anomalies, and incidents.
- **Feedback loop:** Engineers rate diagnoses (helpful/not helpful) to
  improve prompts over time.
- **Multi-cluster:** Support monitoring across multiple k8s clusters / regions.
