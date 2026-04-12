# Log Agent — Intelligent Log Monitor

An intelligent log monitoring agent that tails logs from [Grafana Loki](https://grafana.com/oss/loki/) (or local files), detects anomalies, correlates errors across services, diagnoses root causes using an LLM, and sends actionable alerts via Slack, Teams, email, or stdout.

## Architecture

```
Loki / File ──▶ Filter ──▶ PatternEngine ──▶ Anomaly ──▶ Correlator ──▶ Diagnoser ──▶ Lifecycle ──▶ Dispatcher
 (L1)           (WARN+)     (Drain)          Detector    (dep graph)    (LLM)        Manager       ──▶ Slack
                                             (spike/     (group into    (root cause   (dedup/       ──▶ Teams
                                              new/rate)   incidents)    + severity)    resolve)     ──▶ Email
                                                                                                   ──▶ Log
```

| Stage | Package | Purpose |
|---|---|---|
| **L1: Ingest + Filter** | `internal/ingest/` | Poll Loki or replay files; drop non-error log lines |
| **L2: Pattern Fingerprint** | `internal/pattern/` | Drain algorithm groups logs into templates |
| **L3: Anomaly Detection** | `internal/anomaly/` | Spike, new-pattern, rate-jump detection with EMA baselines |
| **L4: Cross-Service Correlator** | `internal/correlator/` | Group co-occurring anomalies into incidents using dependency graph |
| **L5: LLM Diagnosis** | `internal/diagnosis/` | Send incident to DeepSeek for root cause + severity + fix suggestions |
| **L6: Notify + Dedup** | `internal/notify/` | Incident lifecycle (OPEN→ONGOING→RESOLVED), severity routing, multi-channel dispatch |

## Prerequisites

- Go 1.22+
- A running Loki instance (or use file source for local testing)
- (Optional) DeepSeek API key for LLM diagnosis
- (Optional) Slack webhook URL, Gmail app password, or Teams webhook URL

## Quick Start

```bash
# Clone and build
cd log_agent
go build -o log-agent ./cmd/agent

# Run with local file source (no external dependencies)
./log-agent config/config-file.yaml

# Run with Loki
export GRAFANA_PASSWORD="..."
./log-agent config/config.yaml

# Run with diagnosis (LLM)
export LLM_API_KEY="your-deepseek-key"
./log-agent config/config-diagnosis.yaml

# Run with email notifications
export SMTP_PASSWORD="your-gmail-app-password"
export LLM_API_KEY="your-deepseek-key"
./log-agent config/config-email.yaml
```

## Configuration

The agent reads a YAML config file (default: `config/config.yaml`). Environment variables are expanded automatically (e.g. `${SLACK_WEBHOOK_URL}`).

```yaml
source:
  type: file                             # "loki" (default) or "file"
  file:
    path: testdata/sample_logs.ndjson    # NDJSON file for local testing

loki:
  url: "http://loki.internal:3100"
  query: '{namespace="prod"}'
  poll_interval: 10s
  tenant_id: ""
  service_label: ""
  basic_auth_user: ""
  basic_auth_password: "${GRAFANA_PASSWORD}"

aggregation:
  window: 1m
  min_count: 1

pattern:
  enabled: true
  depth: 4
  similarity: 0.5
  max_children: 100
  max_patterns: 10000
  extract_json_message: true

anomaly:
  enabled: true
  spike_multiplier: 3.0
  rate_jump_factor: 5.0
  ema_alpha: 0.3
  min_samples: 3
  new_pattern_grace: 24h

correlator:
  enabled: true
  window: 2m
  dependencies_file: config/dependencies.yaml

diagnosis:
  enabled: true
  endpoint: https://api.deepseek.com/v1/chat/completions
  model: deepseek-chat
  max_tokens: 1024
  temperature: 0
  timeout: 30s

notification:
  dedup_window: 5m       # suppress duplicate incidents within this window
  resolve_after: 10m     # auto-resolve after silence
  check_interval: 1m     # how often to check for auto-resolve
  channels:
    - type: slack
      webhook_url: "${SLACK_WEBHOOK_URL}"
      severities: [P1, P2, P3]
    - type: email
      smtp_host: "smtp.gmail.com"
      smtp_port: 587
      smtp_username: "alerts@company.com"
      smtp_password: "${SMTP_PASSWORD}"
      from: "alerts@company.com"
      recipients: ["oncall@company.com"]
      severities: [P1, P2]
    - type: teams
      webhook_url: "${TEAMS_WEBHOOK_URL}"
      severities: [P1, P2, P3]
    - type: log                          # always included as fallback
```

### Config Reference

| Section | Field | Description | Default |
|---|---|---|---|
| `source` | `type` | `loki` or `file` | `loki` |
| `source.file` | `path` | NDJSON file path (file source only) | — |
| `loki` | `url` | Loki HTTP base URL | *required for loki* |
| `loki` | `query` | LogQL stream selector | *required for loki* |
| `loki` | `poll_interval` | Poll interval | `10s` |
| `loki` | `tenant_id` | `X-Scope-OrgID` header | — |
| `loki` | `service_label` | Stream label for service name | auto-detect |
| `loki` | `basic_auth_user` | HTTP basic auth user | — |
| `loki` | `basic_auth_password` | HTTP basic auth password | — |
| `aggregation` | `window` | Time window for batching errors | `1m` |
| `aggregation` | `min_count` | Minimum errors before alerting | `1` |
| `pattern` | `enabled` | Enable Drain pattern engine | `false` |
| `pattern` | `depth` | Drain tree depth | `4` |
| `pattern` | `similarity` | Merge threshold (0-1) | `0.5` |
| `anomaly` | `enabled` | Enable anomaly detection | `false` |
| `anomaly` | `spike_multiplier` | σ threshold for spike | `3.0` |
| `anomaly` | `min_samples` | Windows before spike/rate-jump activate | `3` |
| `correlator` | `enabled` | Enable cross-service correlation | `false` |
| `correlator` | `window` | Time window for grouping | `2m` |
| `correlator` | `dependencies_file` | Path to dependency graph YAML | — |
| `diagnosis` | `enabled` | Enable LLM diagnosis | `false` |
| `diagnosis` | `endpoint` | LLM API URL | — |
| `diagnosis` | `model` | Model name | — |
| `notification` | `dedup_window` | Suppress duplicates within this window | `5m` |
| `notification` | `resolve_after` | Auto-resolve after silence | `10m` |
| `notification.channels[]` | `type` | `slack`, `teams`, `email`, or `log` | — |
| `notification.channels[]` | `severities` | Severity filter (empty = all) | all |
| `notification.channels[]` | `webhook_url` | Webhook URL (slack/teams) | — |
| `notification.channels[]` | `smtp_host` | SMTP server (email) | — |
| `notification.channels[]` | `recipients` | Email recipients (email) | — |

### Service Name Extraction

The agent groups errors by **service name**, extracted from Loki stream labels:

- If `service_label` is set (e.g. `"app"`), that label's value is used directly.
- Otherwise, it falls back through: `service` → `app` → `container` → `job` → `"unknown"`.

### Grafana Proxy Mode

If Loki is not directly reachable, you can point the agent at a Grafana datasource proxy:

```yaml
loki:
  url: "http://localhost:3000/api/datasources/proxy/uid/<datasource-uid>"
  basic_auth_user: "admin"
  basic_auth_password: "${GRAFANA_PASSWORD}"
```

Grafana handles the tenant ID header automatically in this mode.

## Level Detection

`ParseLevel` uses a 4-step cascade. If a structured level is found (steps 1–3), it is **authoritative** — no keyword fallback occurs. This prevents false positives like a `debug`-level log mentioning "ERROR" in its message body.

| Step | Format | Example |
|---|---|---|
| 1. JSON | `{"level":"error", ...}` | Also checks `severity`, `log_level` keys |
| 2. Key=Value | `level=error` or `level="error"` | |
| 3. Bracket | `[ERROR]`, `[WARN]` | First known level-word wins |
| 4. Keyword | `ERROR`, `FATAL`, `panic`, `WARN` | Only if no structured level was found |

Recognized aliases: `err` → ERROR, `warning` → WARN, `fatal`/`panic` → FATAL.
Known non-error levels (`info`, `debug`, `trace`) are recognized and **filtered out**.

## Graceful Shutdown

Send `SIGINT` (Ctrl-C) or `SIGTERM`. The agent will:

1. Stop polling Loki
2. Drain in-flight log lines through the filter
3. Flush the current aggregation window (partial alerts are still sent)
4. Exit cleanly

## Running Tests

```bash
go test ./...

# With race detector
go test -race ./...

# Verbose output
go test -v ./...
```

## Project Layout

```
log_agent/
├── cmd/agent/main.go                 # Entry point, config loading, pipeline wiring
├── config/
│   ├── config.yaml                   # Default Loki config
│   ├── config-file.yaml              # Local file-source testing
│   ├── config-correlator.yaml        # Correlator demo
│   ├── config-diagnosis.yaml         # Diagnosis demo (DeepSeek)
│   ├── config-email.yaml             # Email notification demo (Gmail)
│   └── dependencies.yaml             # Service dependency graph
├── internal/
│   ├── ingest/                       # L1: Loki/file source, level filter
│   ├── pattern/                      # L2: Drain algorithm, preprocessing
│   ├── anomaly/                      # L3: Spike/new-pattern/rate-jump detection
│   ├── correlator/                   # L4: Cross-service grouping + dep graph
│   ├── diagnosis/                    # L5: LLM prompt, client, response parser
│   ├── notify/                       # L6: Notifiers, lifecycle, aggregator
│   │   ├── notifier.go               #     Notifier interface, Dispatcher, routing
│   │   ├── incident.go               #     Incident struct, status, ID generation
│   │   ├── lifecycle.go              #     OPEN→ONGOING→RESOLVED state machine
│   │   ├── aggregator.go             #     Time-window alert aggregation
│   │   ├── slack.go                  #     Slack Block Kit webhook
│   │   ├── teams.go                  #     Microsoft Teams Adaptive Card
│   │   ├── email.go                  #     SMTP email (HTML template)
│   │   └── log.go                    #     slog stdout notifier
│   └── testutil/                     # Fake clock, fake Loki, mock notifier
├── testdata/                         # Demo logs, mock servers
├── DESIGN.md                         # Overall architecture (6-layer design)
├── PHASE{1..5}_DESIGN.md             # Per-phase design docs
├── PHASE{1..5}_TEST_PLAN.md          # Per-phase test plans
└── docs/phase6-*.md                  # Phase 6 design & test plan
```
