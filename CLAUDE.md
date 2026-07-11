# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build the single binary
go build -o log-agent ./cmd/agent

# Run against a config (the config path is the sole CLI arg; default config/config.yaml)
./log-agent config/config-demo.yaml          # self-contained demo, no external deps
./log-agent config/config-file.yaml          # local NDJSON file source

# Tests
go test ./...                                 # all packages
go test -race ./...                           # with race detector (pipeline is concurrent — use this)
go test ./internal/anomaly/                   # one package
go test -run TestDetectSpike ./internal/anomaly/   # one test by name
go test -bench . ./internal/ingest/           # benchmarks (bench_test.go in ingest/ and alert/)
```

There is no Makefile, linter config, or codegen. `go vet ./...` and `gofmt` are the conventions.

Go 1.25. The only non-test dependency is `gopkg.in/yaml.v3`; everything else (Drain, Loki client, SMTP, LLM HTTP client) is hand-rolled on the stdlib.

## Architecture

A 6-layer log-monitoring pipeline. **L1–L3 run as one independent, concurrent pipeline per service** (fan-out); their `Alert` channels **fan in** to a single shared **L4–L6** pipeline. A slow/failing source for one service never blocks others.

```
per service:  Source → Filter → PatternEngine → Aggregator → AnomalyDetector ─┐
              (L1)     (L1)      (L2)            (window)     (L3)             │ chan Alert
                                                                              ▼
shared:            MergeAlerts(fan-in) → Correlator → Diagnoser → Lifecycle → Dispatcher
                                         (L4)          (L5 LLM)   (L6 dedup)  (L6 routing)
```

| Layer | Package | Role |
|---|---|---|
| — | `internal/core/` | Shared domain types (`Alert`, `Incident`, `PatternSummary`, `AnomalyKind`) + the `Clock` abstraction (`core.RealClock()`). Imports nothing internal, so every stage can depend on it without a cycle. |
| L1 | `internal/ingest/` | `LogSource` (Loki poll / file replay) + level `Filter` (drops non-error lines). `LogLine` is the core type. |
| L2 | `internal/pattern/` | Drain algorithm groups lines into templates, stamps `PatternID`/`PatternTemplate`. |
| L3 | `internal/anomaly/` | Per-pattern EMA baselines → spike / new-pattern / rate-jump detection → emits `core.Alert`. |
| L3/L4 | `internal/alert/` | `Aggregator` (time-window batching of lines into alerts) + `MergeAlerts` (per-service fan-in). |
| L4 | `internal/correlator/` | Groups co-occurring alerts into a `core.Incident` using a service dependency graph (`depgraph.go`, `config/dependencies.yaml`). |
| L5 | `internal/diagnosis/` | Builds prompt → DeepSeek HTTP call → parses root cause / severity / fix into the incident. |
| L6 | `internal/incident/` | `LifecycleManager` (OPEN→ONGOING→RESOLVED, dedup, auto-resolve). |
| L6 | `internal/notify/` | `Dispatcher` routing by severity to slack/teams/email/log notifiers (implement the `Notifier` interface). |

Everything is wired in [cmd/agent/main.go](cmd/agent/main.go): `loadConfig` → `buildServicePipeline` (per service) → `MergeAlerts` → correlator → diagnoser → lifecycle → dispatch. Each stage past L4 can be bypassed/pass-through when disabled in config.

## Conventions (follow these when adding or editing stages)

- **Channel-stage signature**: `func (s *Stage) Run(ctx, in <-chan In) <-chan Out`. Make the out channel, return it immediately, spawn a goroutine that `defer close(out)`s, and use the **double-select** pattern (`select { case out <- x: case <-ctx.Done(): }`) so both input-close and cancellation are honored. This is uniform across all stages — match it.
- **Clock injection**: any time-dependent component exposes an exported `Clock` field (`core.Clock`, an interface with `Now()`/`After()`) defaulting to `core.RealClock()`; tests inject `testutil.FakeClock`. See `anomaly/detector.go`, `correlator/correlator.go`, `alert/aggregator.go`. (`incident/lifecycle.go` predates the convention and injects a bare `now func() time.Time` instead.)
- **Config structs**: each package owns its `Config` with a `setDefaults()` applying zero-value defaults inside the constructor. The top-level `Config` in main aggregates them. `os.ExpandEnv` expands `${VAR}` in the YAML before unmarshaling.
- **Test doubles** live in `internal/testutil/` (`FakeClock`, `FakeLoki`); mock external servers (LLM, SMTP) are in `testdata/`. Prefer these over real network/time in tests.
- **Level detection** (`ingest`): `ParseLevel` is a 4-step cascade (JSON → key=value → `[BRACKET]` → keyword). A structured level found in steps 1–3 is authoritative — no keyword fallback — so a debug line mentioning "ERROR" isn't misclassified.

## Graceful shutdown

SIGINT/SIGTERM stops polling, drains in-flight lines, flushes the current aggregation window (partial alerts still sent), then exits. Preserve this drain-then-flush ordering when touching the shutdown path in main.

## Docs

`DESIGN.md` is the authoritative architecture doc (§ Concurrency Model, § Buffering & Time Windows). `PHASE{1..5}_*.md` and `docs/phase6-*.md` are historical per-phase design/test plans. `README.md` has the full config reference.
