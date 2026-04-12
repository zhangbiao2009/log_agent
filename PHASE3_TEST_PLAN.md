# Phase 3: Anomaly Detection — Test Plan

**Scope:** Unit tests for `internal/anomaly`, updated tests for `internal/notify`,
and an integration smoke test for the full pipeline with a fake Aggregator.

---

## 1. Guiding Principles

- **No real time.** All tests that involve time use an injectable `Clock`
  or a fixed `time.Time`. No `time.Sleep` in test logic.
- **Deterministic EMA.** Given a fixed sequence of counts and a fixed `α`,
  EMA outcomes are fully deterministic. Tests compute expected values by hand
  or with the same formula as the implementation.
- **Boundary cases first.** For each trigger, test: exactly-at-threshold,
  one-below, one-above.
- **Phase 2 regression.** All existing notify tests must continue to pass
  unchanged. The only new fields on `PatternSummary` (Anomaly, Baseline,
  ZScore) default to zero values — existing test assertions are unaffected.
- **Race detector.** All tests run with `-race`. No shared mutable state
  between goroutines outside of the store (which is single-goroutine by design).

---

## 2. Test Files

```
internal/anomaly/
    baseline_test.go   — PatternBaseline unit tests (21 cases)
    store_test.go      — MemoryStore unit tests (5 cases)
    detector_test.go   — AnomalyDetector channel stage tests (16 cases)
internal/notify/
    notifier_test.go   — AnomalyKind.String() tests (5 cases)
    log_test.go        — (append) anomaly rendering tests (3 cases)
    slack_test.go      — (append) anomaly block tests (3 cases)
```

---

## 3. `baseline_test.go` — PatternBaseline (21 cases)

### 3.1 Update / EMA Convergence (4 cases)

**`TestBaseline_FirstUpdate`**
- Input: blank baseline, first count=50, α=0.3
- Expected: `Mean=50`, `Variance=0`, `N=1`
- Rationale: first observation always sets Mean directly; no variance yet.

**`TestBaseline_SecondUpdate`**
- Input: after first update (Mean=50, Var=0), second count=60, α=0.3
- Expected: `Mean = 50 + 0.3×10 = 53`, `Variance = 0.7×(0+0.3×100) = 21`, `N=2`
- Rationale: verifies the Welford-EMA formula exactly.

**`TestBaseline_ConvergesOnSteadyInput`**
- Feed 30 identical counts of 32 with α=0.3.
- Expected: after 30 updates, `|Mean - 32| < 0.1`, `Variance < 0.5`
- Rationale: EMA on a constant series must converge to that constant.

**`TestBaseline_AdaptsAfterShift`**
- Feed 10 windows of count=30 (baseline established), then 10 windows of count=100.
- Expected: after the 10 high windows, `Mean > 70` (EMA shifted significantly).
- Rationale: EMA must track shifts in underlying rate.

---

### 3.2 Stddev (2 cases)

**`TestBaseline_StddevZeroOnFirstObservation`**
- After first update, `Stddev() == 0`.
- Rationale: variance is 0 after one point; no division by zero.

**`TestBaseline_StddevPositiveAfterVariance`**
- After at least 2 different observations, `Stddev() > 0`.
- Rationale: sanity check that sqrt is applied correctly.

---

### 3.3 IsNewPattern (4 cases)

**`TestBaseline_NewPatternOnFirstObservation`**
- `PatternBaseline{}` — `LastSeen` is zero `time.Time{}` (pattern never seen).
- `IsNewPattern(24h, now)` → `true`.
- Rationale: `time.Since(zero) ≈ 56 years > 24h` → always new.
  (No special N==0 branch needed; the zero-time logic covers it.)

**`TestBaseline_NewPatternAfterGraceExpired`**
- `LastSeen = now - 25h`, grace = `24h`.
- `IsNewPattern(24h, now)` → `true`.
- Rationale: `time.Since(now-25h) = 25h > 24h` → treated as new.
  This is the reappearing-pattern use case.

**`TestBaseline_NotNewPatternWithinGrace`**
- `LastSeen = now - 1h`, grace = `24h`.
- `IsNewPattern(24h, now)` → `false`.
- Rationale: `time.Since(now-1h) = 1h < 24h` → recently seen, not new.

**`TestBaseline_NotNewPatternExactlyAtGraceBoundary`**
- `LastSeen = now - 24h`, grace = `24h`.
- `IsNewPattern(24h, now)` → `false` (boundary is exclusive: `>`, not `>=`).
- Rationale: `time.Since(now-24h) = 24h`, condition is `24h > 24h = false`.

---

### 3.4 IsSpike (5 cases)

**`TestBaseline_NoSpikeBeforeMinSamples`**
- Baseline with `N=3` (below `minSamples=5`), count=1000.
- `IsSpike(1000, 3.0, 5)` → `false`.
- Rationale: spike detection disabled during warmup.

**`TestBaseline_NoSpikeAtExactThreshold`**
- Establish baseline: Mean=32, Stddev≈2 (via 10 updates of 32 with α=0.3).
- Threshold = 32 + 3×2 = 38. Count=38.
- `IsSpike(38, 3.0, 5)` → `false` (not strictly greater than).
- Rationale: threshold is `>`, not `≥`.

**`TestBaseline_SpikeJustAboveThreshold`**
- Same baseline (Mean≈32, Stddev≈2). Count=39.
- `IsSpike(39, 3.0, 5)` → `true`.
- Rationale: one above threshold triggers.

**`TestBaseline_SpikeOnHighCount`**
- Baseline: Mean=32, Stddev≈2. Count=200.
- `IsSpike(200, 3.0, 5)` → `true`.
- Rationale: large spike always triggers.

**`TestBaseline_NoSpikeOnSteadyBaseline`**
- Feed 20 windows of count=32, then check count=33.
- `IsSpike(33, 3.0, 5)` → `false` (minor variance absorbed by EMA).
- Rationale: steady state should not self-trigger.

---

### 3.5 IsRateJump (4 cases)

**`TestBaseline_NoRateJumpBeforeMinSamples`**
- `N=2`, count=1000, factor=5.0.
- `IsRateJump(1000, 5.0, 5)` → `false`.
- Rationale: same warmup guard as Spike.

**`TestBaseline_NoRateJumpWhenMeanIsZero`**
- `Mean=0`, count=10, factor=5.
- `IsRateJump(10, 5.0, 5)` → `false`.
- Rationale: avoid division-by-zero / false positives when mean=0.

**`TestBaseline_RateJumpDetected`**
- `N=10`, Mean=2.0. Count=11. Factor=5.
- `IsRateJump(11, 5.0, 5)` → `true` (11 > 5×2).
- Rationale: 5× jump triggers.

**`TestBaseline_NoRateJumpBelowFactor`**
- `N=10`, Mean=10.0. Count=49. Factor=5.
- `IsRateJump(49, 5.0, 5)` → `false` (49 < 5×10=50).
- Rationale: boundary — 49 is just under 5× mean.

---

### 3.6 ZScore (2 cases)

**`TestBaseline_ZScoreFlooredAtStddevOne`**
- Baseline with `Variance=0` (Stddev=0). Count=100, Mean=10.
- `ZScore(100)` → `(100-10)/max(0,1) = 90`.
- Rationale: stddev floor at 1.0 prevents division by zero.

**`TestBaseline_ZScoreNegativeWhenBelowMean`**
- Mean=50, Stddev=5. Count=35.
- `ZScore(35)` → `(35-50)/5 = -3.0`.
- Rationale: below-mean counts get negative Z-scores.

---

## 4. `store_test.go` — MemoryStore (5 cases)

**`TestMemoryStore_GetMissingKey`**
- `store.Get("nokey")` → `(PatternBaseline{}, false)`.

**`TestMemoryStore_SetAndGet`**
- Set baseline for "pat1". Get returns the same value and `ok=true`.

**`TestMemoryStore_OverwritePreviousValue`**
- Set "pat1" → baseline A. Set "pat1" → baseline B. Get returns B.

**`TestMemoryStore_IndependentKeys`**
- Set "pat1" and "pat2" with different baselines. Each Get returns the correct value.

**`TestMemoryStore_ZeroValueBaseline`**
- Set and retrieve a zero-value `PatternBaseline{}`. Verify no panic or data corruption.

---

## 5. `detector_test.go` — AnomalyDetector (12 cases)

All detector tests use a helper `makeAlert(service, patterns...)` to build
`notify.Alert` values with specific `PatternSummary` entries, and a
`collectAlerts(out, n, timeout)` helper to drain the output channel.

### 5.1 Pipeline Mechanics (3 cases)

**`TestDetector_ClosesOutputWhenInputCloses`**
- Create detector. Close input channel immediately.
- Expected: output channel closes within 1s.
- Rationale: downstream must not block forever.

**`TestDetector_ClosesOutputOnContextCancel`**
- Create detector with cancellable context. Send one alert, cancel ctx.
- Expected: output channel closes within 1s.
- Rationale: graceful shutdown on context cancellation.

**`TestDetector_BufferedOutputSameCapAsInput`**
- Input channel buffered at 10. Verify `cap(out) == cap(in)` (same as PatternEngine convention).
- Rationale: consistent backpressure behaviour across pipeline stages.

---

### 5.2 Suppression (2 cases)

**`TestDetector_SuppressesSteadyStateAlert`**
- Feed 10 windows of the same pattern with count=32 to warm up baselines.
- Feed an 11th window with count=32.
- Expected: no alert forwarded on the 11th window.
- Rationale: core value proposition of L3.

**`TestDetector_ForwardsAlertIfAnyPatternAnomalous`**
- Alert with 3 patterns: pat1 (steady), pat2 (steady), pat3 (new).
- Expected: alert is forwarded. pat1 and pat2 have `Anomaly=AnomalyNone`;
  pat3 has `Anomaly=AnomalyNewPattern`.
- Rationale: one anomalous pattern is sufficient to forward the whole alert.

---

### 5.3 NewPattern trigger (2 cases)

**`TestDetector_FirstObservationIsNewPattern`**
- Alert with a pattern ID that the store has never seen.
- Expected: alert forwarded with `PatternSummary.Anomaly = AnomalyNewPattern`.
- Rationale: basic NewPattern trigger.

**`TestDetector_PatternReappearsAfterGrace`**
- Pre-seed store with baseline for patID, `LastSeen = now - 25h`, grace=24h.
- Send one alert with that patID.
- Expected: forwarded with `AnomalyNewPattern`.
- Rationale: `time.Since(LastSeen)=25h > grace=24h` → long-absent pattern treated as new.

---

### 5.4 Spike trigger (2 cases)

**`TestDetector_SpikeAnnotatedOnPatternSummary`**
- Warm up with 10 windows of count=32.
- Send one window with count=200.
- Expected: alert forwarded; `PatternSummary.Anomaly = AnomalySpike`,
  `ZScore > 3.0`, `Baseline ≈ 32.0`.
- Rationale: `Baseline` and `ZScore` are snapshotted **before** `baseline.Update` is
  called, so they reflect the historical mean the spike was compared against, not
  the post-update mean. Baseline ≈ 32 not ≈ 82.

**`TestDetector_NoSpikeBeforeMinSamples`**
- Send 3 windows of count=32 (below `minSamples=5`), then count=200.
- Expected: all suppressed or forwarded only for NewPattern (first window),
  NOT for Spike. The 4th window (steady) → suppressed.
- Rationale: warmup guard prevents false positives on startup.

---

### 5.5 RateJump trigger (2 cases)

**`TestDetector_RateJumpDetected`**
- Warm up 10 windows at count=2. Send count=15 (7.5× mean, above factor=5).
- Expected: alert forwarded with `Anomaly=AnomalyRateJump`.
- Rationale: low-mean pattern detects large relative jumps.

**`TestDetector_NoRateJumpBelowFactor`**
- Warm up 10 windows at count=10. Send count=30 (3× mean, below factor=5).
- Expected: alert suppressed (neither Spike nor RateJump).
- Rationale: moderate increase should not trigger rate jump.

---

### 5.6 Baseline updates (3 cases)

**`TestDetector_BaselineIsUpdatedAfterEachWindow`**
- Send 5 windows of count=50 to a blank store.
- Inspect store after the 5th window: `Mean` should be closer to 50 than to 0.
- Expected: `|store.Get(patID).Mean - 50| < 15`.
- Rationale: each forwarded (or suppressed) window must still update the baseline.

**`TestDetector_BaselineUpdatedForSuppressedWindows`** *(missing test MT-3)*
- Warm up 10 windows of count=32 (establishes baseline, N=10, all forwarded as NewPattern+steady).
- Send 5 more windows of count=32 → all suppressed (AnomalyNone).
- After the 5 suppressed windows, inspect `store.Get(patID).N` → expected 15.
- Rationale: **critical correctness property**: suppressed windows must still call
  `baseline.Update`. If they don't, `N` stays at 10 and the EMA never converges
  on the true steady-state rate. This is the test most likely to catch a missing
  `Update` call inside the `AnomalyNone` branch.

**`TestDetector_SpikeWinsOverRateJump`** *(missing test MT-1)*
- Warm up 10 windows at count=10 (Mean≈10, Stddev≈0.5 after convergence).
- Send count=200: satisfies both Spike (200 > 10+3×0.5) AND RateJump (200 > 5×10=50).
- Expected: `PatternSummary.Anomaly = AnomalySpike`, not `AnomalyRateJump`.
- Rationale: priority rule `NewPattern > Spike > RateJump` (as documented in the design)
  must be enforced. Spike takes precedence over RateJump.

**`TestDetector_Phase1AlertsForwardedAsIs`** *(missing test MT-2)*
- Build `notify.Alert{Patterns: nil}` (or `Patterns: []PatternSummary{}`).
- Send through the detector.
- Expected: alert forwarded unchanged; no panic; zero anomaly classification.
- Rationale: when the pattern engine is disabled (Phase 1 mode), alerts have no
  PatternSummary entries. The detector has no PatternID to look up, so it must
  forward the alert as-is without modification.

---

## 6. Notifier tests (notify package additions)

### 6.1 `notifier_test.go` — AnomalyKind.String() (5 cases)

```go
TestAnomalyKind_String:
  AnomalyNone.String()       → "none"
  AnomalyNewPattern.String() → "new_pattern"
  AnomalySpike.String()      → "spike"
  AnomalyRateJump.String()   → "rate_jump"
  AnomalyKind(99).String()   → "unknown(99)"   // unknown value, no panic
```

### 6.2 `log_test.go` — anomaly rendering (3 cases)

**`TestLogNotifier_PatternWithSpikeAnomaly`**
- Alert with one pattern: `Anomaly=AnomalySpike`, `ZScore=4.2`, Count=200.
- Expected log output contains "SPIKE" and "4.2".
- Rationale: spike is visible in log output.

**`TestLogNotifier_PatternWithNewPattern`**
- Alert with one pattern: `Anomaly=AnomalyNewPattern`.
- Expected log output contains "NEW" or "new_pattern".
- Rationale: new pattern badge is visible.

**`TestLogNotifier_PatternWithNoAnomaly`**
- Alert with one pattern: `Anomaly=AnomalyNone` (i.e., detector disabled,
  zero value). Expected: no anomaly tag in output.
- Rationale: zero-value Anomaly must not add noise to Phase 2 output.

### 6.3 `slack_test.go` — anomaly blocks (3 cases)

**`TestSlackNotifier_SpikePatternHasEmoji`**
- Alert with `Anomaly=AnomalySpike`. Format the Slack message.
- Expected: at least one block's text contains the spike emoji or "SPIKE".
- Rationale: anomaly badges are rendered in Slack.

**`TestSlackNotifier_NewPatternHasEmoji`**
- Alert with `Anomaly=AnomalyNewPattern`.
- Expected: text contains the new-pattern emoji or "NEW".

**`TestSlackNotifier_NoAnomalyPatternHasNoEmoji`**
- Alert with `Anomaly=AnomalyNone`. Ensure no anomaly emoji appears.
- Rationale: steady-state pattern blocks must not have spurious badges.

---

## 7. Integration Smoke Test

**File:** `internal/anomaly/integration_test.go`

**`TestDetector_EndToEnd_SteadyThenSpike`**

Setup:
- `AnomalyConfig{SpikeMultiplier:3.0, EMAAlpha:0.3, MinSamples:5, NewPatternGrace:24h, RateJumpFactor:5.0}`
- Use a fake clock set to `now`.
- **Warm-up strategy**: send 20 alerts with count=32 through the detector's `Run()` pipeline
  itself (drain all output in a goroutine). This naturally creates `AnomalyNewPattern` for
  the first window, then `AnomalyNone` for subsequent ones. Do NOT manipulate internal struct
  fields directly — that leaks the `PatternBaseline` API and makes tests brittle.
- After warm-up: discard all output from those windows.

Steps:
1. Send 5 alerts with count=32 (baseline well-established → suppressed, 0 forwarded).
2. Send 1 alert with count=200 → forwarded with `AnomalySpike`.
3. Send 3 more alerts with count=32 → suppressed again.

Assertions:
- Exactly 1 alert forwarded (the spike).
- That alert's `PatternSummary[0].Anomaly == AnomalySpike`.
- `ZScore > 3.0` (reflecting pre-update mean of ≈32, not the post-update shifted mean).

---

## 8. Benchmark

**`BenchmarkDetector_Ingest`** in `detector_test.go`:

```go
func BenchmarkDetector_Ingest(b *testing.B) {
    // Pre-warm 1000 pattern baselines.
    // Feed b.N alerts, each with 10 patterns, alternating patternIDs.
    // Measure: ns/op for the evaluate() path.
}
```

Target: < 10µs per alert with 10 patterns (map lookup + EMA math is O(n_patterns)).

---

## 9. What We Do Not Test Here

| Concern | Reason omitted |
|---|---|
| Time-of-day baseline accuracy | Not implemented in Phase 3 |
| SQLite persistence | MemoryStore only in Phase 3 |
| Concurrent store access | Store is single-goroutine by design; race detector covers it |
| Config parsing (main.go wiring) | Covered by build test + manual smoke test |

---

## 10. Test Execution Checklist

```bash
# Unit tests with race detector
go test -race ./internal/anomaly/... ./internal/notify/...

# Full suite regression check
go test -race ./...

# Benchmarks
go test ./internal/anomaly/ -run ^$ -bench=BenchmarkDetector -benchtime=5s

# Manual smoke test against real Loki
# Expected: first window = NewPattern alert (baselines empty)
# Subsequent windows = silent (steady state)
GRAFANA_PASSWORD=secret go run ./cmd/agent/
```
