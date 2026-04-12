# Phase 2: Pattern Fingerprint — Test Plan

**Component under test:** Pattern detection (Drain algorithm) and modified
aggregation pipeline.
**Run command:** `cd log_agent && go test ./...`
**Prerequisite:** All Phase 1 tests still pass.

---

## 1. Unit Tests — `internal/pattern/preprocess_test.go`

### TC-P01: JSON message extraction

| # | Input | Expected Output | Notes |
|---|---|---|---|
| 1 | `{"level":"error","msg":"connection timeout","err":"dial tcp 10.0.0.1:5432: i/o timeout"}` | `connection timeout dial tcp <IP>:<NUM>: i/o timeout` | Extracts `msg` + `err`, normalizes IP and port |
| 2 | `{"level":"error","message":"request failed"}` | `request failed` | `message` key variant |
| 3 | `{"level":"error","error":"EOF"}` | `EOF` | `error` key variant |
| 4 | `{"level":"error","msg":"fail","method":"HandleUpdate"}` | `fail HandleUpdate` | Also extracts `method` for context |
| 5 | `{"level":"error","msg":""}` | `` | Empty message |
| 6 | `not json at all` | `not json at all` | Non-JSON passes through unchanged |
| 7 | `{malformed` | `{malformed` | Invalid JSON passes through unchanged |

### TC-P02: IPv4 address normalization

| # | Input | Expected Output |
|---|---|---|
| 1 | `connection to 10.0.0.1 failed` | `connection to <IP> failed` |
| 2 | `dial tcp 192.168.1.100:5432` | `dial tcp <IP>:<NUM>` |
| 3 | `no ip here` | `no ip here` |

> **Note:** IPv6 normalization is deferred — the regex is error-prone and
> IPv4 covers the majority of real-world logs.

### TC-P03: UUID normalization

| # | Input | Expected Output |
|---|---|---|
| 1 | `request 550e8400-e29b-41d4-a716-446655440000 failed` | `request <UUID> failed` |
| 2 | `id=A1B2C3D4-E5F6-7890-ABCD-EF1234567890` | `id=<UUID>` |
| 3 | `no uuid here` | `no uuid here` |

### TC-P04: Number normalization

| # | Input | Expected Output |
|---|---|---|
| 1 | `timeout after 3000ms` | `timeout after <NUM>ms` |
| 2 | `latency 3.14s` | `latency <NUM>s` |
| 3 | `status 500` | `status <NUM>` |
| 4 | `port 8080` | `port <NUM>` |
| 5 | `error code -1` | `error code -<NUM>` |
| 6 | `no numbers here` | `no numbers here` |

### TC-P05: Hex string normalization

| # | Input | Expected Output |
|---|---|---|
| 1 | `object 0x7f4a2b3c9d00 freed` | `object <HEX> freed` |
| 2 | `sha=abcdef1234567890` | `sha=<HEX>` |
| 3 | `short ab12 stays` | `short ab12 stays` | Too short (<8 chars), kept |

### TC-P06: Combined normalization

| # | Input | Expected Output |
|---|---|---|
| 1 | `dial tcp 10.0.0.1:5432: request 550e8400-e29b-41d4-a716-446655440000 timeout 3000ms` | `dial tcp <IP>:<NUM>: request <UUID> timeout <NUM>ms` |
| 2 | `{"level":"error","msg":"dial tcp 10.0.0.1:5432 timeout","err":"i/o timeout"}` | `dial tcp <IP>:<NUM> timeout i/o timeout` |

### TC-P07: Real notifications-processor logs

| # | Input | Expected Output |
|---|---|---|
| 1 | `{"containerVersion":"Container version environment variable unknown.","err":"Operation cannot be fulfilled on eventsubtypes.main.notifications.infoblox.com \"comingsoon\": the object has been modified; please apply your changes to the latest version and try again","level":"error","method":"HandleUpdateEventSubtypeCRD","msg":"EventSubtypes.UpdateStatus fail","request_id":"","time":"2026-04-12T02:00:20Z"}` | `EventSubtypes.UpdateStatus fail HandleUpdateEventSubtypeCRD Operation cannot be fulfilled on eventsubtypes.main.notifications.infoblox.com "comingsoon": the object has been modified; please apply your changes to the latest version and try again` |
| 2 | Same as above but with `"comingsoon"` replaced by `"policy-rules-order-v2"` | Should differ only in the quoted resource name |

---

## 2. Unit Tests — `internal/pattern/drain_test.go`

### TC-D01: Single line creates a new pattern

**Setup:** `NewDrain(DefaultConfig)`. Process: `"connection timeout to host-1"`.
**Expected:** Returns a `Pattern` with `Template = "connection timeout to host-1"`,
`TokenCount = 4`. Pattern ID is non-empty.

### TC-D02: Identical lines match the same pattern

**Setup:** Process the same string 5 times.
**Expected:** All return the same Pattern (same ID). Only one pattern in the
Drain catalog.

### TC-D03: Similar lines merge into a template

**Setup:** Process sequence:
1. `"connection timeout to host-1"`
2. `"connection timeout to host-2"`

**Expected:** Both return the same pattern. Template becomes
`"connection timeout to <*>"`. Pattern ID updated.

### TC-D04: Dissimilar lines create separate patterns

**Setup:** Process:
1. `"connection timeout to host-1"`
2. `"disk full on /dev/sda1"`

**Expected:** Two distinct patterns (different IDs).

### TC-D05: Preprocessed wildcards treated as matching tokens

**Setup:** Process two already-preprocessed lines:
1. `"failed to connect to <IP> port <NUM> timeout <NUM>ms"`
2. `"failed to connect to <IP> port <NUM> timeout <NUM>ms"`

**Expected:** Same pattern (identical tokens). Drain treats `<IP>`, `<NUM>`,
etc. as regular tokens for matching — they're constant after preprocessing.

### TC-D06: Token count branching

**Setup:** Process:
1. `"timeout"` (1 token)
2. `"connection timeout"` (2 tokens)

**Expected:** Two different patterns — different token counts go to
different branches.

### TC-D07: Similarity threshold — at boundary

**Setup:** Drain with `SimilarityThreshold = 0.5`. Process a 4-token line,
then a 4-token line where exactly 2 tokens differ.

Similarity = 2/4 = 0.5, which meets the threshold.

**Expected:** Merged into one pattern.

### TC-D08: Similarity threshold — below boundary

**Setup:** Same as TC-D07 but 3 tokens differ. Similarity = 1/4 = 0.25.

**Expected:** Two separate patterns.

### TC-D09: Wildcard in existing template matches any token

**Setup:** Process:
1. `"cannot connect to host-1"` → template: `"cannot connect to host-1"`
2. `"cannot connect to host-2"` → template merges: `"cannot connect to <*>"`
3. `"cannot connect to host-xyz"` → should match existing template

**Expected:** Third line returns the same pattern as the merged template.
The `<*>` slot matches `"host-xyz"` without reducing similarity.

### TC-D10: Max children — overflow to wildcard

**Setup:** Drain with `MaxChildren = 3`. Process lines that would create
4 children at the same tree level:
1. `"error in alpha module"`
2. `"error in beta module"`
3. `"error in gamma module"`
4. `"error in delta module"`

**Expected:** Lines 1-3 create normal children. Line 4 goes through the
`<*>` wildcard child. All may end up matching one pattern depending on
similarity.

### TC-D11: Max patterns — LRU eviction

**Setup:** Drain with `MaxPatterns = 3`. Create 4 distinct patterns
(dissimilar enough not to merge).

**Expected:** After the 4th pattern is created, only 3 patterns remain.
The least recently matched is evicted. The newest pattern is present.

### TC-D12: Empty input

**Setup:** Process an empty string.

**Expected:** Returns a pattern (single pattern for empty lines) or
handles gracefully. No panic.

### TC-D13: Single-token input

**Setup:** Process `"ERROR"`.

**Expected:** Creates a pattern with 1 token. Tree navigates correctly
with depth 4 (shallow path).

### TC-D14: Pattern ID stability

**Setup:** Process same line 100 times.

**Expected:** Pattern ID is the same on every call. Template doesn't
change if no merge occurs.

### TC-D15: Pattern ID changes on merge

**Setup:**
1. Process `"timeout to host-1"` → Pattern A with ID `id1`
2. Process `"timeout to host-2"` → merges, template changes to `"timeout to <*>"`

**Expected:** Returned pattern has a new ID (hash of new template). The old
ID is no longer in the catalog.

### TC-D16: Concurrent safety — caller is responsible

**Note:** This is a documentation test. Drain is explicitly NOT thread-safe.
Verify the exported doc comment says so.

### TC-D17: Real-world pattern convergence

**Setup:** Feed 50 variations of notifications-processor errors through
Preprocess + Drain:
- 30x `EventSubtypes.UpdateStatus fail` with different resource names
- 15x `EventSubtypes.Update fail` with different validation messages
- 5x completely different errors

**Expected:**
- At most 3-5 patterns emerge (not 50)
- The UpdateStatus errors converge to one template
- The Update/validation errors converge to one template
- The 5 miscellaneous errors may each be their own pattern

---

## 3. Unit Tests — `internal/pattern/engine_test.go`

### TC-E01: Basic flow — lines enriched with PatternID

**Setup:** Create PatternEngine. Send 5 LogLines through.
**Expected:** All output lines have non-empty `PatternID`. Other fields
(Service, Timestamp, Level, Raw) are unchanged.

### TC-E02: Same error gets same PatternID

**Setup:** Send 3 lines with same Raw text.
**Expected:** All 3 output lines have the same PatternID.

### TC-E03: Different errors get different PatternIDs

**Setup:** Send 2 lines with very different Raw text.
**Expected:** Different PatternIDs.

### TC-E04: Context cancellation

**Setup:** Start engine, cancel context before all input is consumed.
**Expected:** Output channel closes. No goroutine leak.

### TC-E05: Input channel closes

**Setup:** Send 3 lines, then close input channel.
**Expected:** Output channel emits 3 lines and then closes.

### TC-E06: JSON logs — message extraction

**Setup:** Send a JSON log line.
**Expected:** PatternID is based on the extracted message content, not the
full JSON string.

### TC-E07: Preserves original Raw field

**Setup:** Send a JSON log line.
**Expected:** `Raw` field in the output line is still the original JSON
string (not the preprocessed version).

### TC-E08: JSON extraction disabled

**Setup:** Create PatternEngine with `ExtractJSONMessage: false`. Send a
JSON log line.
**Expected:** PatternID is based on the full raw JSON string, not just
extracted message fields. Pattern template includes JSON structure tokens.

---

## 4. Modified Tests — `internal/notify/aggregator_test.go`

### TC-A-P01: Groups by (service, patternID)

**Setup:** Send 10 lines: 7 with `PatternID="abc"` and 3 with
`PatternID="def"`, all for `service-a`. Flush window.

**Expected:** One Alert with `Service="service-a"`, `Count=10`, and
`Patterns` containing 2 entries:
- `{Template: ..., Count: 7}`
- `{Template: ..., Count: 3}`
Sorted by count descending.

### TC-A-P02: Empty PatternID — backwards compatible

**Setup:** Send 5 lines with `PatternID=""` (Phase 1 mode).

**Expected:** Alert has `Count=5`, `Patterns` is empty (nil or len 0),
and `SampleLines` contains up to 5 raw examples (Phase 1 behavior).

### TC-A-P03: Multiple services with patterns

**Setup:** Send lines for `service-a` (2 patterns) and `service-b`
(1 pattern).

**Expected:** 2 Alerts. `service-a` has 2 pattern summaries.
`service-b` has 1 pattern summary.

### TC-A-P04: Severity per pattern

**Setup:** Send lines for `service-a`: 3 WARN with pattern A,
2 ERROR with pattern B.

**Expected:** Alert has `Level="ERROR"` (overall highest).
Pattern A has `Level="WARN"`. Pattern B has `Level="ERROR"`.

### TC-A-P05: Pattern sample lines capped at 3

**Setup:** Send 20 lines with the same pattern.

**Expected:** The PatternSummary has `Count=20` and
`SampleLines` has at most 3 entries.

### TC-A-P06: Existing aggregator tests still pass

**Verify:** All Phase 1 aggregator tests (TC-A01 through TC-A11) continue
to pass, since LogLines without PatternID should still work.

---

## 5. Modified Tests — `internal/notify/log_test.go`

### TC-LOG-P01: Pattern output format

**Setup:** Alert with 2 patterns.

**Expected:** Output includes:
```
ALERT service=... level=ERROR count=... patterns=2
  [7x ERROR] connection timeout to <*>
  [3x WARN]  disk usage at <NUM>%
```

### TC-LOG-P02: No patterns — fallback format

**Setup:** Alert with `Patterns=nil`.

**Expected:** Falls back to Phase 1 style (raw sample lines).

---

## 6. Modified Tests — `internal/notify/slack_test.go`

### TC-SLACK-P01: Pattern blocks

**Setup:** Alert with 2 patterns. Capture POST body.

**Expected:** JSON body contains:
- Header with service name and total count
- Per-pattern sections with template, count, and sample

### TC-SLACK-P02: No patterns — fallback

**Setup:** Alert with `Patterns=nil`.

**Expected:** Falls back to Phase 1 format.

---

## 7. Benchmarks — `internal/pattern/bench_test.go`

### BenchmarkPreprocess

**Input:** 1000 iterations of a real JSON log line from notifications-processor.

**Target:** < 5μs/op. Must not allocate more than 3 allocs/op.

### BenchmarkDrainProcess

**Input:** Pre-process 1000 variations of the same pattern (different
variable values), process through Drain.

**Targets:**
- Matching an existing pattern: < 2μs/op
- Creating a new pattern: < 5μs/op

### BenchmarkDrainProcess_HighCardinality

**Input:** 5000 completely unique log lines (worst case — every line is a
new pattern up to `maxPatterns`).

**Target:** < 10μs/op amortized (including LRU eviction overhead).

### BenchmarkPatternEngine_EndToEnd

**Input:** 5000 LogLines through the full engine (preprocess + drain).

**Target:** Total < 50ms (10μs/line budget). This is well under the 30s
poll interval — the pipeline won't become a bottleneck.

---

## 8. Integration Test

### TC-INT01: Full pipeline with pattern detection

**Setup:** httptest fake Loki returning 100 log lines:
- 60x similar error A (with different variable parts)
- 30x similar error B
- 10x similar error C

Run full pipeline: LokiSource → Filter → PatternEngine → Aggregator → MockNotifier.
Wait for one window flush.

**Expected:** MockNotifier receives 1 Alert with approximately 3 patterns:
- Pattern for error A, count ~60
- Pattern for error B, count ~30
- Pattern for error C, count ~10
Total count = 100.

### TC-INT02: Pattern disabled — Phase 1 behavior

**Setup:** Same as TC-INT01 but `pattern.enabled = false`.

**Expected:** MockNotifier receives Alert with Count=100 and no pattern
breakdown (same as Phase 1).

---

## 9. Test Summary

| Package | File | Test Count |
|---|---|---|
| `pattern` | `preprocess_test.go` | 17 |
| `pattern` | `drain_test.go` | 17 |
| `pattern` | `engine_test.go` | 8 |
| `notify` | `aggregator_test.go` | +6 (modified) |
| `notify` | `log_test.go` | +2 (modified) |
| `notify` | `slack_test.go` | +2 (modified) |
| `pattern` | `bench_test.go` | 4 |
| integration | `engine_test.go` | 2 |
| **Total** | | **~58 tests** |
