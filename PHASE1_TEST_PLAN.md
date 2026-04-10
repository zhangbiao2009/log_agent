# Phase 1: Error Catcher — Test Plan

**Component under test:** Log ingestion pipeline (LokiSource → Filter → Aggregator → Dispatcher → Notifiers)  
**Run command:** `cd log_agent && go test ./...`  
**Integration tests:** `go test -tags=integration ./...`

---

## 1. Unit Tests

### 1.1 `internal/ingest/filter_test.go`

#### TC-F01: ParseLevel — JSON structured logs

| # | Input | Expected Level | Notes |
|---|---|---|---|
| 1 | `{"level":"error","msg":"db timeout"}` | `ERROR` | lowercase `level` key |
| 2 | `{"severity":"FATAL","msg":"oom killed"}` | `FATAL` | `severity` key variant |
| 3 | `{"log_level":"warn","msg":"slow query"}` | `WARN` | `log_level` key variant |
| 4 | `{"level":"info","msg":"started"}` | `""` | INFO must be dropped |
| 5 | `{"level":"debug","msg":"trace data"}` | `""` | DEBUG must be dropped |
| 6 | `{"level":"ERROR","msg":"uppercase"}` | `ERROR` | uppercase value |
| 7 | `{"level":"Error","msg":"mixed case"}` | `ERROR` | mixed case value |
| 8 | `{"msg":"no level field"}` | `""` | JSON without level key → fallback to keyword scan |
| 9 | `{"level":"","msg":"empty level"}` | `""` | empty level value |

#### TC-F02: ParseLevel — bracket format

| # | Input | Expected Level |
|---|---|---|
| 1 | `2026-04-09 14:30:00 [ERROR] connection refused` | `ERROR` |
| 2 | `2026-04-09 14:30:00 [WARN] slow query 3.2s` | `WARN` |
| 3 | `2026-04-09 14:30:00 [FATAL] out of memory` | `FATAL` |
| 4 | `2026-04-09 14:30:00 [INFO] request complete` | `""` |
| 5 | `2026-04-09 14:30:00 [DEBUG] cache hit` | `""` |

#### TC-F03: ParseLevel — key-value format

| # | Input | Expected Level |
|---|---|---|
| 1 | `ts=2026-04-09 level=error msg="timeout"` | `ERROR` |
| 2 | `ts=2026-04-09 level=fatal msg="crash"` | `FATAL` |
| 3 | `ts=2026-04-09 level=warn msg="retry"` | `WARN` |
| 4 | `ts=2026-04-09 level=info msg="ok"` | `""` |

#### TC-F04: ParseLevel — keyword fallback

| # | Input | Expected Level | Notes |
|---|---|---|---|
| 1 | `ERROR - failed to connect to redis` | `ERROR` | bare keyword |
| 2 | `goroutine 1 [running]: panic: nil pointer` | `FATAL` | `panic` maps to FATAL |
| 3 | `FATAL: cannot bind to port 8080` | `FATAL` | |
| 4 | `WARN: disk usage at 92%` | `WARN` | |
| 5 | `request completed 200 OK` | `""` | no error keywords |
| 6 | `all systems operational` | `""` | no match |

#### TC-F05: ParseLevel — edge cases

| # | Input | Expected Level | Notes |
|---|---|---|---|
| 1 | `""` | `""` | empty string |
| 2 | `"   "` | `""` | whitespace only |
| 3 | `{malformed json` | fall through to keyword scan | invalid JSON |
| 4 | string with 10KB of text containing `ERROR` at the end | `ERROR` | large input |
| 5 | `ERRORS_TOTAL metric updated` | `ERROR` | substring match (acceptable false positive for Phase 1) |
| 6 | `user error: invalid email format` | `ERROR` | keyword appears mid-sentence |

#### TC-F06: Filter — drops non-error lines

**Setup:** Create a buffered input channel. Push 10 log lines: 3 ERROR, 2 FATAL, 1 WARN, 4 INFO. Close input.  
**Expected:** Output channel emits exactly 6 lines (3+2+1). All have Level ∈ {ERROR, FATAL, WARN}. Output channel closes after input is drained.

#### TC-F07: Filter — empty input

**Setup:** Create input channel, close immediately.  
**Expected:** Output channel closes without emitting anything. No goroutine leak.

#### TC-F08: Filter — context cancellation

**Setup:** Create input channel with data. Start Filter. Cancel context before all lines are consumed.  
**Expected:** Output channel closes. No goroutine leak. No panic.

#### TC-F09: Filter — preserves metadata

**Setup:** Push a LogLine with `Service="payment-service"`, `Timestamp=T`, `Raw="[ERROR] timeout"`.  
**Expected:** Output line has identical `Service`, `Timestamp`, and `Raw`.

---

### 1.2 `internal/ingest/loki_test.go`

#### TC-L01: ParseResponse — valid Loki `query_range` response

**Setup:** Canned JSON matching Loki's response format:
```json
{
  "status": "success",
  "data": {
    "resultType": "streams",
    "result": [
      {
        "stream": {"service": "payment-service", "namespace": "prod"},
        "values": [
          ["1712678400000000000", "2026-04-09 [ERROR] connection refused"],
          ["1712678401000000000", "2026-04-09 [INFO] health check ok"]
        ]
      }
    ]
  }
}
```
**Expected:** 2 LogLine objects. First: `Service="payment-service"`, `Raw` contains "connection refused". Second: `Raw` contains "health check ok". Timestamps parsed correctly as `time.Time`.

#### TC-L02: ParseResponse — multiple streams (services)

**Setup:** Response with 2 stream entries (payment-service, order-service).  
**Expected:** LogLines have correct Service for each.

#### TC-L03: ParseResponse — empty result

**Setup:** Valid Loki response with `"result": []`.  
**Expected:** Zero LogLines returned. No error.

#### TC-L04: ParseResponse — malformed JSON

**Setup:** Invalid JSON body.  
**Expected:** Returns error. No panic.

#### TC-L05: HighWaterMark — deduplication across polls

**Setup:** First poll returns timestamps [T1, T2, T3]. Second poll returns [T2, T3, T4, T5] (overlap).  
**Expected:** Combined output: [T1, T2, T3, T4, T5]. No duplicates for T2, T3.

#### TC-L06: HighWaterMark — first poll starts from now

**Setup:** Create LokiSource. Inspect the query parameters of the first HTTP request.  
**Expected:** `start` parameter is approximately `time.Now()` (within a few seconds).

#### TC-L07: HTTP error → retry

**Setup:** `httptest.Server` returns 500 on first request, 200 on second.  
**Expected:** LokiSource retries. LogLines from the second response are emitted. A warning is logged for the first failure.

#### TC-L08: HTTP timeout → retry

**Setup:** `httptest.Server` delays 30s on first request (exceeds client timeout).  
**Expected:** LokiSource times out, retries. No hang.

#### TC-L09: Consecutive failures → error log

**Setup:** `httptest.Server` returns 500 three times in a row.  
**Expected:** After the 3rd failure, an error-level (not just warning) log is emitted. Agent does NOT crash; continues retrying.

#### TC-L10: Query parameters

**Setup:** Start LokiSource, capture HTTP request with `httptest.Server`.  
**Expected:** Request path is `/loki/api/v1/query_range`. Query params include `query`, `start`, `end`, `direction=forward`.

---

### 1.3 `internal/notify/aggregator_test.go`

> **Testing time:** Aggregator accepts a `Clock` interface. Tests inject a
> fake clock to control window boundaries without `time.Sleep`.

```go
type Clock interface {
    Now() time.Time
    NewTicker(d time.Duration) *time.Ticker
}
```

#### TC-A01: Batches by service

**Setup:** Push 10 ERROR lines for `service-a` and 5 for `service-b`. Advance clock past 1 window.  
**Expected:** 2 alerts emitted. `service-a.Count=10`, `service-b.Count=5`.

#### TC-A02: Window boundaries

**Setup:** Push 3 lines at T+5s, 2 more at T+30s. Advance clock to T+60s (window boundary). Then push 1 more at T+65s, advance to T+120s.  
**Expected:** First flush: 1 alert with Count=5. Second flush: 1 alert with Count=1.

#### TC-A03: SampleLines capped at 5

**Setup:** Push 20 ERROR lines from `service-a` in one window.  
**Expected:** `alert.SampleLines` has exactly 5 entries. `alert.Count` is 20.

#### TC-A04: SampleLines content

**Setup:** Push 3 lines with distinct `Raw` text.  
**Expected:** `SampleLines` contains all 3 raw texts.

#### TC-A05: MinCount threshold — below

**Setup:** MinCount=5. Push 3 lines.  
**Expected:** No alert emitted on window flush.

#### TC-A06: MinCount threshold — at boundary

**Setup:** MinCount=5. Push exactly 5 lines.  
**Expected:** Alert emitted with Count=5.

#### TC-A07: Severity — highest wins

**Setup:** Push 3 WARN and 1 FATAL from same service.  
**Expected:** `alert.Level = "FATAL"`. Severity order: FATAL > ERROR > WARN.

#### TC-A08: Empty window

**Setup:** No log lines. Advance clock past window.  
**Expected:** No alert emitted (no empty alerts).

#### TC-A09: Flush on shutdown

**Setup:** Push 3 lines. Cancel context before window expires.  
**Expected:** Partial window flushed: alert emitted with Count=3.

#### TC-A10: Multiple windows

**Setup:** Push lines across 3 consecutive windows.  
**Expected:** 3 separate flushes, each with correct counts.

#### TC-A11: Output channel closes

**Setup:** Cancel context, drain all alerts.  
**Expected:** Alert output channel is closed. No goroutine leak.

---

### 1.4 `internal/notify/dispatcher_test.go`

#### TC-D01: Fan-out to all notifiers

**Setup:** 3 mock notifiers. Dispatch 1 alert.  
**Expected:** All 3 received exactly the same alert.

#### TC-D02: One failure does not block others

**Setup:** Mock A returns error, Mock B returns nil.  
**Expected:** Mock B receives the alert. Dispatch returns an error (aggregated).

#### TC-D03: All failures

**Setup:** 2 mock notifiers, both return errors.  
**Expected:** Dispatch returns error. Both errors are logged.

#### TC-D04: Timeout — slow notifier

**Setup:** Mock A sleeps 30s. Mock B returns immediately.  
**Expected:** Mock B receives the alert. Mock A is cancelled after 10s timeout. Dispatch completes in ~10s, not 30s.

#### TC-D05: Zero notifiers

**Setup:** Dispatcher with no notifiers.  
**Expected:** Dispatch returns nil. No panic.

#### TC-D06: Context cancellation

**Setup:** Cancel parent context before dispatch.  
**Expected:** Dispatch returns context error. Does not hang.

---

### 1.5 `internal/notify/slack_test.go`

#### TC-S01: Message format — normal alert

**Setup:** `httptest.Server` captures POST body. Send alert: service="payment-service", Count=47, 3 sample lines.  
**Expected:**
- POST to webhook URL
- Content-Type: `application/json`
- Body contains service name "payment-service"
- Body contains "47"
- Body contains all 3 sample lines
- Valid JSON (parseable)

#### TC-S02: Message format — zero samples

**Setup:** Alert with Count=1, SampleLines=[].  
**Expected:** Valid message sent. No "Samples:" section (or section is empty).

#### TC-S03: Message format — special characters

**Setup:** Sample line contains `<`, `>`, `&`, `"`, newlines.  
**Expected:** Characters are escaped properly in JSON. No broken Slack blocks.

#### TC-S04: HTTP 200 → success

**Setup:** `httptest.Server` returns 200.  
**Expected:** `Send()` returns nil.

#### TC-S05: HTTP 500 → error

**Setup:** `httptest.Server` returns 500 with body "server error".  
**Expected:** `Send()` returns error containing status code.

#### TC-S06: HTTP 429 (rate limited)

**Setup:** `httptest.Server` returns 429 with `Retry-After` header.  
**Expected:** `Send()` returns error (Phase 1 does not retry; retries added later).

#### TC-S07: Connection refused

**Setup:** Point WebhookURL to a closed port.  
**Expected:** `Send()` returns error. No panic.

#### TC-S08: Invalid webhook URL

**Setup:** WebhookURL = "not-a-url".  
**Expected:** `Send()` returns error.

---

### 1.6 `internal/notify/log_test.go`

#### TC-LN01: LogNotifier outputs alert

**Setup:** Create LogNotifier with a `bytes.Buffer`-backed logger. Send alert.  
**Expected:** Buffer contains service name, count, and level.

#### TC-LN02: LogNotifier — multiple alerts

**Setup:** Send 3 alerts with different services.  
**Expected:** Buffer contains 3 separate log entries.

---

## 2. Integration Tests

> Build tag: `//go:build integration`  
> Run: `go test -tags=integration ./...`

### TC-INT01: Full pipeline — fake Loki → Filter → Aggregator → MockNotifier

**Setup:**
1. `httptest.Server` serves canned Loki response: 10 ERROR, 5 INFO, 2 FATAL across 2 services.
2. Wire: `LokiSource(fakeLoki) → Filter → Aggregator(500ms window) → Dispatcher(MockNotifier)`.
3. Run for 3 seconds, then cancel context.

**Expected:**
- MockNotifier received alerts.
- No alert has Level = "INFO".
- Count totals match: 10 ERROR + 2 FATAL = 12 error lines distributed across services.
- Each alert has ≤ 5 SampleLines.
- Output channel closed cleanly.

### TC-INT02: Full pipeline — Loki returns no errors

**Setup:** Fake Loki serves only INFO/DEBUG lines.  
**Expected:** No alerts emitted. Pipeline exits cleanly on context cancel.

### TC-INT03: Full pipeline — Loki goes down mid-stream

**Setup:**
1. Fake Loki serves valid response on first poll.
2. Returns 500 on second poll.
3. Returns valid response on third poll.

**Expected:**
- Alerts emitted from poll 1 and 3.
- No alert from poll 2 (error logged, not crashed).
- Pipeline stays alive across the failure.

### TC-INT04: Full pipeline — multiple notifiers with partial failure

**Setup:** Dispatcher has SlackNotifier (pointed at fake that returns 500) + LogNotifier.  
**Expected:** LogNotifier receives all alerts. Slack errors are logged but don't block delivery.

### TC-INT05: Graceful shutdown

**Setup:** Pipeline is running, consuming from fake Loki. Send context cancel.  
**Expected:**
- Aggregator flushes partial window.
- All channels close.
- `main` exits with code 0.
- Verified with `goleak.VerifyNone(t)`: no goroutine leaks.

---

## 3. Manual Tests

### 3.1 Smoke test — real Loki, log-only output

**Prerequisites:** Access to a Loki instance with some services generating logs.

```bash
cd log_agent && go build -o agent ./cmd/agent/

# Run with log notifier only
LOKI_URL=http://localhost:3100 ./agent --config config/config.yaml
```

**Verify:**
- [ ] Agent starts without error
- [ ] Stdout shows `ALERT service=<name> level=<level> count=<n>` lines within ~1 minute of errors appearing in Loki
- [ ] No alerts for services with only INFO/DEBUG logs
- [ ] Ctrl-C stops the agent cleanly (no hanging, no goroutine dump)

### 3.2 Smoke test — error injection

```bash
# In another terminal, generate errors in a test service:
for i in $(seq 1 50); do
  curl -s http://localhost:8080/debug/error > /dev/null
  sleep 0.1
done
```

**Verify:**
- [ ] Agent emits an alert within the configured window (default 1m)
- [ ] Alert count is approximately 50
- [ ] Sample lines show the actual error messages

### 3.3 Slack delivery

**Prerequisites:** Slack workspace with a `#bot-testing` channel and a webhook URL.

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../xxx"
./agent --config config/config.yaml
```

**Verify:**
- [ ] Message appears in `#bot-testing` within ~1 minute
- [ ] Message contains service name, error count, and sample lines
- [ ] Emoji/formatting renders correctly (no broken markdown)

### 3.4 Resilience — Loki restart

```bash
# Start agent, then restart Loki
docker restart loki
```

**Verify:**
- [ ] Agent logs warnings during Loki downtime
- [ ] Agent resumes processing after Loki comes back
- [ ] No duplicate alerts from logs already processed before restart

### 3.5 Resilience — Slack webhook down

**Verify:**
- [ ] Set `SLACK_WEBHOOK_URL` to an invalid URL
- [ ] Agent logs errors for Slack delivery failure
- [ ] LogNotifier still outputs alerts to stdout
- [ ] Agent does not crash or hang

---

## 4. Acceptance Criteria

Phase 1 is **done** when all of these pass:

| # | Criterion | Verified By |
|---|---|---|
| AC-1 | `go test ./...` passes with 0 failures | CI |
| AC-2 | `go test -tags=integration ./...` passes | CI |
| AC-3 | `go vet ./...` reports no issues | CI |
| AC-4 | `golangci-lint run` reports no issues | CI |
| AC-5 | Agent connects to real Loki and emits log alerts to stdout | Manual 3.1 |
| AC-6 | Agent sends Slack messages with correct formatting | Manual 3.3 |
| AC-7 | Agent survives Loki downtime without crashing | Manual 3.4 |
| AC-8 | Agent survives Slack failure without blocking other notifiers | Manual 3.5 |
| AC-9 | Agent shuts down cleanly on SIGINT (no goroutine leaks) | TC-INT05 |
| AC-10 | No secrets in config files (webhook URLs from env vars) | Code review |

---

## 5. Test Helpers to Build

These are reusable across Phase 1 tests and later phases.

### 5.1 `internal/testutil/mock_notifier.go`

```go
type MockNotifier struct {
    SendFunc func(ctx context.Context, alert notify.Alert) error
    Alerts   []notify.Alert // collected alerts
    mu       sync.Mutex
}

func (m *MockNotifier) Send(ctx context.Context, a notify.Alert) error {
    m.mu.Lock()
    m.Alerts = append(m.Alerts, a)
    m.mu.Unlock()
    if m.SendFunc != nil {
        return m.SendFunc(ctx, a)
    }
    return nil
}

func (m *MockNotifier) Name() string { return "mock" }
```

### 5.2 `internal/testutil/fake_clock.go`

```go
type FakeClock struct {
    now    time.Time
    mu     sync.Mutex
    tickers []*FakeTicker
}

func (c *FakeClock) Now() time.Time { ... }
func (c *FakeClock) Advance(d time.Duration) { ... } // triggers tickers
func (c *FakeClock) NewTicker(d time.Duration) *FakeTicker { ... }
```

### 5.3 `internal/testutil/fake_loki.go`

```go
// NewFakeLoki returns an httptest.Server that serves canned Loki responses.
// Call AddResponse() to enqueue responses for successive polls.
func NewFakeLoki() *FakeLoki

func (f *FakeLoki) AddResponse(lines []LogEntry)
func (f *FakeLoki) URL() string
func (f *FakeLoki) Close()
func (f *FakeLoki) RequestCount() int // how many polls happened
```

---

## 6. Coverage Target

| Package | Target | Notes |
|---|---|---|
| `internal/ingest` | ≥ 85% | `ParseLevel` table-driven tests cover most branches |
| `internal/notify` | ≥ 80% | Slack HTTP edge cases are hard to exhaust |
| Overall | ≥ 80% | Measured with `go test -coverprofile=cover.out ./...` |
