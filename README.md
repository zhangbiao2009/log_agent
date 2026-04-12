# Log Agent — Error Catcher

A lightweight agent that tails logs from [Grafana Loki](https://grafana.com/oss/loki/), detects error-level entries, aggregates them over a configurable time window, and sends alerts to Slack (or stdout).

## Architecture

```
Loki ──poll──▶ LokiSource ──▶ Filter ──▶ Aggregator ──▶ Dispatcher ──▶ Slack / Log
               (query_range)   (ParseLevel)  (time window)   (fan-out)
```

1. **LokiSource** polls Loki's `query_range` API on a fixed interval, tracking a high-water mark to avoid duplicates.
2. **Filter** parses each log line for a severity level (JSON → key=value → `[BRACKET]` → keyword fallback) and drops anything below WARN.
3. **Aggregator** buckets matching lines per service over a time window, tracks the highest severity, and emits an `Alert` when the window closes.
4. **Dispatcher** fans out each alert to all configured notifiers concurrently.

## Prerequisites

- Go 1.22+
- A running Loki instance (or any Loki-compatible API)

## Quick Start

```bash
# Clone and build
cd log_agent
go build -o log-agent ./cmd/agent

# Set your Slack webhook (optional — falls back to stdout logging)
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../xxx"

# Run with default config
./log-agent

# Or specify a custom config path
./log-agent /path/to/config.yaml
```

## Configuration

The agent reads a YAML config file (default: `config/config.yaml`). Environment variables in the file are expanded automatically (e.g. `${SLACK_WEBHOOK_URL}`).

```yaml
loki:
  url: "http://loki.internal:3100"   # Loki base URL (or Grafana proxy URL)
  query: '{namespace="prod"}'         # LogQL stream selector
  poll_interval: 10s                  # How often to poll (default: 10s)
  tenant_id: ""                       # X-Scope-OrgID header for multi-tenant Loki
  service_label: ""                   # Label key to extract service name (see below)
  basic_auth_user: ""                 # HTTP basic auth username (e.g. for Grafana proxy)
  basic_auth_password: "${GRAFANA_PASSWORD}"  # HTTP basic auth password

aggregation:
  window: 1m        # Aggregation window duration (default: 1m)
  min_count: 1       # Minimum error count to trigger an alert (default: 1)

notification:
  channels:
    - type: slack
      webhook_url: "${SLACK_WEBHOOK_URL}"
    - type: log      # Prints alerts to stdout via slog
```

### Config Reference

| Section | Field | Description | Default |
|---|---|---|---|
| `loki` | `url` | Loki HTTP base URL (or Grafana datasource proxy URL) | *required* |
| `loki` | `query` | LogQL stream selector | *required* |
| `loki` | `poll_interval` | Poll interval (Go duration) | `10s` |
| `loki` | `tenant_id` | `X-Scope-OrgID` header for multi-tenant Loki | — |
| `loki` | `service_label` | Stream label key to use as service name | auto-detect |
| `loki` | `basic_auth_user` | HTTP basic auth username | — |
| `loki` | `basic_auth_password` | HTTP basic auth password | — |
| `aggregation` | `window` | Time window for batching errors | `1m` |
| `aggregation` | `min_count` | Minimum errors before alerting | `1` |
| `notification.channels[]` | `type` | `slack` or `log` | — |
| `notification.channels[]` | `webhook_url` | Slack incoming webhook URL (slack only) | — |

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
├── cmd/agent/main.go          # Entry point, config loading, pipeline wiring
├── config/config.yaml         # Default configuration
├── internal/
│   ├── ingest/
│   │   ├── source.go          # LogLine struct, LogSource interface
│   │   ├── filter.go          # ParseLevel, Filter channel function
│   │   ├── loki.go            # LokiSource (polling, response parsing)
│   │   ├── filter_test.go     # 22 ParseLevel cases + Filter tests
│   │   └── loki_test.go       # Response parsing, service fallback, streaming
│   ├── notify/
│   │   ├── notifier.go        # Alert struct, Notifier interface, Dispatcher
│   │   ├── aggregator.go      # Time-window aggregation with Clock interface
│   │   ├── slack.go           # Slack Block Kit formatting
│   │   ├── log.go             # slog-based notifier
│   │   ├── aggregator_test.go # Window flush, severity ranking, thresholds
│   │   ├── dispatcher_test.go # Fan-out, partial/total failure
│   │   ├── slack_test.go      # Block Kit, HTML escaping, error handling
│   │   └── log_test.go        # Output verification
│   └── testutil/
│       ├── fake_clock.go      # Deterministic time for aggregator tests
│       └── fake_loki.go       # httptest-based fake Loki server
├── DESIGN.md                  # Overall architecture (6-layer design)
├── PHASE1_DESIGN.md           # Phase 1 detailed design
└── PHASE1_TEST_PLAN.md        # Phase 1 test plan
```
