# Log Agent Workspace Instructions

## Project Overview

This is an intelligent log monitoring agent written in Go that tails logs from Grafana Loki (or local files), detects anomalies, correlates errors across microservices, diagnoses root causes using an LLM, and sends actionable alerts via multiple channels (Slack, Teams, email, stdout).

**Six-Phase Pipeline Architecture:**
1. **L1: Ingest + Filter** (`internal/ingest/`) — Poll Loki or replay files; drop non-error logs
2. **L2: Pattern Fingerprint** (`internal/pattern/`) — Drain algorithm groups logs into templates
3. **L3: Anomaly Detection** (`internal/anomaly/`) — Spike, new-pattern, rate-jump detection with EMA baselines
4. **L4: Cross-Service Correlator** (`internal/correlator/`) — Group co-occurring anomalies into incidents using dependency graph
5. **L5: LLM Diagnosis** (`internal/diagnosis/`) — Send incident to DeepSeek for root cause + severity + fix suggestions
6. **L6: Notify + Dedup** (`internal/notify/`) — Incident lifecycle (OPEN→ONGOING→RESOLVED), severity routing, multi-channel dispatch

## Architecture Principles

### Channel-Based Pipeline Pattern

All pipeline stages follow a consistent channel-based design:

```go
// Standard pipeline stage signature
func (s *Stage) Run(ctx context.Context, in <-chan InputType) <-chan OutputType {
    out := make(chan OutputType, cap(in))
    go func() {
        defer close(out)
        for {
            select {
            case <-ctx.Done():
                return
            case item, ok := <-in:
                if !ok {
                    return
                }
                processed := s.process(item)
                select {
                case out <- processed:
                case <-ctx.Done():
                    return
                }
            }
        }
    }()
    return out
}
```

**Key conventions:**
- Always accept `context.Context` as first parameter
- Return output channel immediately (non-blocking)
- Pipeline stage runs in goroutine spawned by `Run()`
- Always `defer close(out)` at start of goroutine
- Handle both `ctx.Done()` and input channel closure
- Use double-select pattern to respect context cancellation when sending

### Testability via Clock Injection

Components that depend on time should have an injectable clock interface:

```go
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
}

type MyComponent struct {
    Clock Clock  // exported for test injection; defaults to realClock{}
}

type realClock struct{}
func (realClock) Now() time.Time { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
```

See `internal/anomaly/detector.go`, `internal/correlator/correlator.go`, `internal/notify/lifecycle.go` for examples.

### Configuration Pattern

All components use a `Config` struct with zero-value defaults applied in the constructor:

```go
type ComponentConfig struct {
    Threshold float64
    Window    time.Duration
}

func (c *ComponentConfig) setDefaults() {
    if c.Threshold == 0 {
        c.Threshold = 3.0
    }
    if c.Window == 0 {
        c.Window = 2 * time.Minute
    }
}

func NewComponent(cfg ComponentConfig) *Component {
    cfg.setDefaults()
    return &Component{config: cfg}
}
```

**YAML config in main:**
- Environment variable expansion using `os.ExpandEnv()` before unmarshaling
- Top-level `Config` struct in `cmd/agent/main.go` aggregates all sub-configs
- Each package defines its own config struct (e.g., `AnomalyConfig`, `PatternConfig`)

## Code Organization

### Package Structure

```
internal/
  ingest/       — Log sources (Loki, File) + LogLine type
  pattern/      — Drain algorithm (pattern extraction)
  anomaly/      — Baseline tracking + spike/rate-jump detection
  correlator/   — Dependency graph + incident grouping
  diagnosis/    — LLM client + prompt building + response parsing
  notify/       — Alert/Incident types, lifecycle manager, dispatchers, channels
  testutil/     — Test doubles (FakeClock, FakeLoki, MockNotifier)
cmd/
  agent/        — Main entry point, config loading, pipeline wiring
config/         — Example YAML configs
testdata/       — Sample logs, mock servers (mock_llm_server.go, mock_smtp_server.go)
docs/           — Design docs for each phase
```

### File Naming Conventions

- `<feature>.go` — Main implementation
- `<feature>_test.go` — Unit tests
- `bench_test.go` — Benchmarks (separate from unit tests)
- Interface definitions in the file where they're primarily used
- Test utilities in `internal/testutil/` for reuse across packages

## Testing Standards

### Test Function Naming

Use descriptive, underscore-separated names that describe the behavior:

```go
func TestDrain_SameLineProducesSamePattern(t *testing.T) {}
func TestLifecycle_DuplicateWithinWindow_Suppressed(t *testing.T) {}
func TestAnomalyDetector_SpikeTriggersAlert(t *testing.T) {}
```

### Test Structure

Follow table-driven tests when appropriate, but prefer focused single-scenario tests for clarity:

```go
func TestComponent_SpecificBehavior(t *testing.T) {
    // Setup
    cfg := ComponentConfig{Threshold: 2.0}
    comp := NewComponent(cfg)
    
    // Execute
    result := comp.Process(input)
    
    // Verify
    if result != expected {
        t.Errorf("got %v, want %v", result, expected)
    }
}
```

### Testing Channel Pipelines

Use helper patterns for testing `Run()` methods:

```go
func TestStage_ProcessesItems(t *testing.T) {
    ctx := context.Background()
    in := make(chan InputType, 10)
    
    stage := NewStage(config)
    out := stage.Run(ctx, in)
    
    // Send test data
    in <- testInput
    close(in)
    
    // Collect results
    var results []OutputType
    for item := range out {
        results = append(results, item)
    }
    
    // Verify
    if len(results) != 1 {
        t.Errorf("got %d results, want 1", len(results))
    }
}
```

### Fake Implementations for Testing

Provide test doubles in `internal/testutil/`:
- `FakeClock` — deterministic time control
- `FakeLoki` — in-memory Loki fake for integration tests
- `MockNotifier` — tracks notification calls

## Common Patterns

### Interface Design

Keep interfaces small and focused:

```go
// Good: Single responsibility
type LogSource interface {
    Stream(ctx context.Context) (<-chan LogLine, error)
}

type Notifier interface {
    Send(ctx context.Context, incident Incident) error
    Name() string
}

type BaselineStore interface {
    Get(service, patternID string) (Baseline, bool)
    Update(service, patternID string, baseline Baseline)
}
```

### Error Handling

- Return errors from constructors and initial operations (e.g., `Stream()`)
- Log errors within pipeline stages using `slog` — don't propagate via channels
- Use `slog.Warn()` for recoverable errors (e.g., LLM unavailable), `slog.Error()` for critical failures

```go
if err := d.client.Complete(ctx, prompt); err != nil {
    slog.Warn("LLM diagnosis failed, using heuristic", "err", err)
    // Continue with fallback logic
}
```

### Struct Field Ordering

1. Immutable config/dependencies
2. Mutable state (protected by mutex if concurrent)
3. Exported test hooks (e.g., `Clock Clock`)

```go
type Component struct {
    config ComponentConfig    // immutable after construction
    store  BaselineStore       // dependency
    mu     sync.Mutex          // protects mutable state below
    cache  map[string]Data
    Clock  Clock                // exported for test injection
}
```

## Domain-Specific Patterns

### Pattern ID Generation

All pattern IDs are SHA-256 hashes (first 12 hex chars) of the template:

```go
func generatePatternID(template string) string {
    h := sha256.Sum256([]byte(template))
    return fmt.Sprintf("%x", h[:6])
}
```

This ensures deterministic IDs across restarts.

### Anomaly Annotation Pattern

The anomaly detector enriches alerts in-place:

```go
// Anomaly detector receives Alert with Patterns filled in
// and adds Anomaly/Baseline/ZScore fields to each PatternSummary
for i := range alert.Patterns {
    alert.Patterns[i].Anomaly = AnomalySpike
    alert.Patterns[i].Baseline = baseline.Mean
    alert.Patterns[i].ZScore = zScore
}
```

### Incident Correlation

Incidents use deterministic IDs based on sorted service names + time window:

```go
func GenerateIncidentID(services []string, openedAt time.Time, window time.Duration) string {
    sorted := make([]string, len(services))
    copy(sorted, services)
    sort.Strings(sorted)
    floor := openedAt.Truncate(window)
    payload := strings.Join(sorted, ",") + "|" + floor.UTC().Format(time.RFC3339Nano)
    h := sha256.Sum256([]byte(payload))
    return fmt.Sprintf("%x", h[:6])
}
```

### Lifecycle State Machine

Incidents follow a strict state machine (see `internal/notify/lifecycle.go`):

```
OPEN → ONGOING → RESOLVED
  ↑________________↑ (new event resets state)
```

**Transitions:**
- First occurrence → OPEN (emit)
- Duplicate within dedup window → no emit
- Duplicate after dedup window → ONGOING (emit)
- No new events for `resolve_after` duration → RESOLVED (emit)

## Dependencies

- **Standard library only** for core functionality (no external dependencies except yaml)
- `gopkg.in/yaml.v3` — YAML config parsing
- No logging framework except stdlib `log/slog`
- HTTP client for Loki/LLM uses `net/http`
- No third-party testing frameworks — use stdlib `testing`

## Code Style

### Logging

Use structured logging with `slog`:

```go
slog.Info("diagnosis complete",
    "severity", severity,
    "services", inc.Services,
)

slog.Warn("LLM diagnosis failed, using heuristic", "err", err)
```

### Comments

- Package comments at top of primary file (e.g., `drain.go`, `detector.go`)
- Exported functions/types require doc comments
- Algorithm explanations in code (see Drain, EMA baseline updates)
- TODO comments reference phase number: `// TODO(phase7): implement priority queue`

### Naming

- **Packages:** lowercase, singular noun (e.g., `pattern`, `anomaly`, not `patterns`)
- **Types:** PascalCase (e.g., `AnomalyDetector`, `LifecycleManager`)
- **Interfaces:** -er suffix when possible (e.g., `Notifier`, `Diagnoser`)
- **Config structs:** `<Component>Config` (e.g., `DrainConfig`, `AnomalyConfig`)
- **Test helpers:** `default<Type>()`, `fake<Interface>()` (lowercase helpers)

### Line Length and Formatting

- Use `gofmt` / `goimports` for formatting (run before commits)
- Prefer readability over strict line length limits
- Break long function signatures at parameter boundaries

## Design Documents

Each phase has detailed design docs:
- `PHASE1_DESIGN.md` → `PHASE5_DESIGN.md` — Incremental feature design
- `docs/phase6-notify-dedup-design.md` — Latest phase documentation
- Reference these for context on architectural decisions

## Extension Points

When adding new features:

### New Log Source

Implement `ingest.LogSource`:

```go
type MySource struct{}

func (s *MySource) Stream(ctx context.Context) (<-chan ingest.LogLine, error) {
    // Your implementation
}
```

### New Notification Channel

Implement `notify.Notifier`:

```go
type MyNotifier struct{}

func (n *MyNotifier) Send(ctx context.Context, incident notify.Incident) error {
    // Your implementation
}

func (n *MyNotifier) Name() string {
    return "my-notifier"
}
```

### New Anomaly Detection Algorithm

Follow the pattern in `internal/anomaly/detector.go`:
- Store baselines in `BaselineStore`
- Annotate `PatternSummary.Anomaly` field
- Emit only anomalous alerts

## Performance Considerations

- **Channel buffer sizes:** Use `cap(in)` when creating output channels to match upstream capacity
- **Baseline storage:** In-memory map is acceptable for up to 10k patterns (see `max_patterns` config)
- **LLM calls:** Diagnoser runs in pipeline (blocks); consider async pool if latency becomes issue
- **Loki polling:** Configured via `poll_interval` (default 30s); too frequent causes API rate limits

## Common Gotchas

1. **Context propagation:** Always pass `ctx` through the pipeline; interrupting source cancels entire chain
2. **Channel closure:** Only the sender should close a channel; `Run()` methods defer close at start of goroutine
3. **Zero config values:** Always apply defaults in constructor; don't rely on YAML defaults
4. **Time-dependent tests:** Use `FakeClock` instead of `time.Sleep()` for deterministic tests
5. **Incident ID collisions:** Unlikely with SHA-256, but time-window truncation ensures uniqueness within correlation window

## AI Assistant Guidance

When assisting with this codebase:

1. **Maintain phase progression:** New features should follow the six-phase architecture
2. **Test coverage:** Every new pipeline stage needs channel-based tests
3. **Config compatibility:** Add new fields to config structs with zero-value defaults
4. **Documentation:** Update relevant `PHASE*_DESIGN.md` or create new docs in `docs/`
5. **Interface stability:** Avoid breaking changes to `LogLine`, `Alert`, `Incident` types
6. **Naming consistency:** Follow the `<Component>Config`, `New<Component>()` pattern
7. **No external deps:** Prefer stdlib solutions unless there's strong justification

## Quick Reference

**Build:** `go build -o log-agent ./cmd/agent`  
**Test:** `go test ./...`  
**Run:** `./log-agent config/config.yaml`  
**Benchmark:** `go test -bench=. -benchmem ./internal/pattern`  
**Config validation:** All configs use `os.ExpandEnv()` for `${VAR}` expansion
