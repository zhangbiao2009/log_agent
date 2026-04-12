# Phase 2: Pattern Fingerprint — Detailed Design

**Goal:** Add a Drain-based log pattern detection layer between Filter and
Aggregator. Instead of alerting on raw error counts per service, the agent
groups identical error patterns together and reports *kinds* of errors with
their counts. This dramatically reduces alert noise.

**Example:** Instead of "notifications-processor: 34 errors", the agent
reports:
```
notifications-processor: 2 error patterns detected
  [28x] EventSubtypes.UpdateStatus fail: Operation cannot be fulfilled on <*> "<*>": the object has been modified
  [ 6x] EventSubtypes.Update fail: <*> is invalid: [spec.eventtype: Invalid value: <*>]
```

**Prerequisite:** Phase 1 (Error Catcher) — completed.

---

## 1. Pipeline Change

### Before (Phase 1)

```
LokiSource → Filter → Aggregator → Dispatcher → Notifiers
                          │
                    groups by: service
                    emits: Alert{Count, SampleLines}
```

### After (Phase 2)

```
LokiSource → Filter → PatternEngine → Aggregator → Dispatcher → Notifiers
                           │                │
                      assigns PatternID      groups by: (service, pattern)
                      to each LogLine        emits: Alert{Patterns}
```

**Key change:** A `PatternEngine` channel stage sits between Filter and
Aggregator. It enriches each `LogLine` with a `PatternID` field (a hash of
the templatized pattern). The Aggregator then groups by `(service, PatternID)`
instead of just `service`, and the Alert struct carries per-pattern details.

---

## 2. Package: `internal/pattern`

New package — separate from `ingest` and `notify` because pattern detection
is a distinct concern that will also be used by L3 (anomaly detection).

### 2.1 Types

```go
// pattern.go

// Pattern represents a templatized log pattern discovered by Drain.
type Pattern struct {
    ID         string    // hex-encoded hash of the template tokens
    Template   string    // human-readable template, e.g. "connection timeout to <*>:<*>"
    Tokens     []string  // tokenized template, variable positions are "<*>"
    TokenCount int       // number of tokens (used for tree indexing)
    LastMatched time.Time // last time this pattern was matched (for LRU eviction)
}
```

### 2.2 Pre-processing

Before tokenizing, apply regex-based normalization to collapse obvious
variables. This improves pattern stability (prevents templates bloating
with minor numeric differences).

```go
// preprocess.go

// Preprocess normalizes variable parts of a log line before tokenization.
// Applied in order:
//   1. Strip JSON structure — extract only the message value(s)
//   2. IPv4 addresses → <IP>
//   3. UUIDs → <UUID>
//   4. Hex strings (≥8 chars) → <HEX>
//   5. Numbers (integers and decimals) → <NUM>
func Preprocess(raw string) string
```

**JSON log handling:** Most of the logs we see are JSON (e.g. the
notifications-processor). Before Drain processes a log line:
1. Try to parse as JSON.
2. If successful, extract the `msg` (or `message`, `error`, `err`) field
   value(s) and concatenate them. This is the string we feed to Drain.
3. This prevents JSON key names, timestamps, and structural characters
   from polluting templates.

Example:
```
Input:  {"level":"error","method":"HandleUpdateEventSubtypeCRD","msg":"EventSubtypes.UpdateStatus fail","err":"Operation cannot be fulfilled on eventsubtypes.main.notifications.infoblox.com \"comingsoon\": the object has been modified"}
After:  EventSubtypes.UpdateStatus fail Operation cannot be fulfilled on eventsubtypes.main.notifications.infoblox.com "comingsoon": the object has been modified
```

**Why not just pattern on the raw JSON?** The JSON keys and timestamps are
noise. Two log lines with identical messages but different timestamps would
be 80% different tokens if we include all the JSON, potentially creating
separate templates.

### 2.3 Drain Algorithm

**Reference:** He et al., "Drain: An Online Log Parsing Approach with
Fixed Depth Tree" (ICWS 2017).

```go
// drain.go

// DrainConfig controls Drain's behavior.
type DrainConfig struct {
    Depth               int     // fixed tree depth (default: 4)
    SimilarityThreshold float64 // merge threshold (default: 0.5)
    MaxChildren         int     // max children per tree node (default: 100)
    MaxPatterns         int     // max total patterns to track (default: 10000)
    ExtractJSONMessage  bool    // extract msg/err from JSON before Drain (default: true)
}

// Drain is an online log parser that discovers patterns.
// It is NOT safe for concurrent use — the caller must serialize access.
type Drain struct {
    config   DrainConfig
    root     *node
    patterns map[string]*Pattern // ID → Pattern
    idLookup map[string]string   // template string → Pattern ID (for dedup)
}

func NewDrain(cfg DrainConfig) *Drain

// Process takes a preprocessed log string (NOT raw JSON) and returns
// the matching Pattern. If no existing pattern matches, a new one is
// created. Thread-unsafe — caller synchronizes.
func (d *Drain) Process(preprocessed string) *Pattern
```

**How the tree works:**

```
root
 └── [token_count = 7]
      └── ["EventSubtypes.UpdateStatus"]    ← first token
           └── ["fail"]                      ← second token
                └── leaf → clusters: [Pattern{...}]
```

1. **Tokenize** the preprocessed string by whitespace.
2. **Level 1:** Look up (or create) child by token count.
3. **Levels 2..depth-1:** Look up child by the exact token at that position.
   If the token is a wildcard (`<*>`, `<NUM>`, etc.) or the node already has
   `maxChildren` children, use the special `<*>` child.
4. **Leaf level:** Iterate over clusters (pattern candidates). Compute
   similarity: `matchingTokens / totalTokens`. If similarity ≥ threshold,
   merge (replace differing tokens with `<*>`). Return the pattern.
5. **No match:** Create a new pattern from the token sequence. Add to leaf.

**Pattern ID:** `hex(fnv1a(template_string))`. Recomputed whenever the
template changes due to a merge, so old IDs are retired and lookups updated.

**Merging example:**
```
Existing: "cannot be fulfilled on <*> <*> the object has been modified"
New line: "cannot be fulfilled on eventsubtypes.main.notifications \"comingsoon\" the object has been modified"
Result:   "cannot be fulfilled on <*> <*> the object has been modified"  (already generalized, no change)
```

**Memory bound:** `maxPatterns` limits total patterns. When exceeded, the
least-recently-used pattern is evicted (replaced by the new one). This
prevents memory growth from cardinality explosions.

### 2.4 PatternEngine (Channel Stage)

```go
// engine.go

// PatternEngine wraps Drain and provides the channel-based pipeline stage.
type PatternEngine struct {
    drain  *Drain
    preproc func(string) string // Preprocess function
}

func NewPatternEngine(cfg DrainConfig) *PatternEngine

// Run consumes filtered log lines and emits them with PatternID
// and PatternTemplate set.
// The output LogLine has the same fields as the input, plus pattern fields.
func (e *PatternEngine) Run(ctx context.Context, in <-chan ingest.LogLine) <-chan ingest.LogLine
```

**Thread safety:** `Run` uses a single goroutine, so `Drain.Process()` is
called sequentially. No mutex needed.

---

## 3. Changes to Existing Types

### 3.1 LogLine — add PatternID

```go
// source.go (modified)

type LogLine struct {
    Service         string
    Timestamp       time.Time
    Level           string
    Raw             string
    PatternID       string // set by PatternEngine (empty if pattern detection disabled)
    PatternTemplate string // human-readable template (set alongside PatternID)
}
```

Adding fields to a struct is backwards-compatible. Phase 1 code that
doesn't set `PatternID` / `PatternTemplate` still works — they're just empty.
The Aggregator uses `PatternTemplate` to populate `PatternSummary.Template`
at flush time.

### 3.2 Alert — add Patterns

Add `Patterns` field to `Alert`. Keep `SampleLines` for backwards
compatibility when PatternEngine is disabled.

```go
// notifier.go (modified)

type Alert struct {
    Service     string
    Level       string        // highest severity across all patterns
    Count       int           // total error lines (sum of all pattern counts)
    Window      time.Duration
    Timestamp   time.Time
    SampleLines []string         // up to 5 raw examples (used when Patterns is empty)
    Patterns    []PatternSummary // per-pattern breakdown (sorted by count desc)
}

type PatternSummary struct {
    Template    string   // e.g. "EventSubtypes.UpdateStatus fail: <*>"
    Count       int
    Level       string   // highest severity for this pattern
    SampleLines []string // up to 3 raw examples
}
```

**Which field do Notifiers use?**
- If `Patterns` is non-empty → render per-pattern breakdown.
- If `Patterns` is empty (PatternEngine disabled) → fall back to
  `Alert.SampleLines` (Phase 1 behavior).

### 3.3 Aggregator — group by (service, patternID)

The aggregator bucket key changes from `service` to `service + "|" + patternID`.
When PatternID is empty (Phase 1 fallback), behavior is identical to before.

```go
// aggregator.go (modified)

func bucketKey(line ingest.LogLine) string {
    if line.PatternID != "" {
        return line.Service + "|" + line.PatternID
    }
    return line.Service
}
```

**Flush algorithm (changed from Phase 1):**

On flush, re-aggregate per service — collect all pattern buckets for the
same service into a single Alert with `Patterns` sorted by count desc.

```
flush(buckets):
  serviceMap = map[service] → { level, count, patterns[], samples[] }

  for key, bucket in buckets:
    service, patternID = splitBucketKey(key)  // split on "|"
    sa = serviceMap[service]
    sa.count += bucket.count
    sa.level  = max(sa.level, bucket.level)

    if patternID != "":
      sa.patterns = append(sa.patterns, PatternSummary{
        Template:    bucket.template,    // captured from first LogLine
        Count:       bucket.count,
        Level:       bucket.level,
        SampleLines: bucket.samples[:3],
      })
    else:
      sa.samples = bucket.samples        // Phase 1 fallback

  for service, sa in serviceMap:
    sort sa.patterns by count descending
    emit Alert{
      Service:     service,
      Level:       sa.level,
      Count:       sa.count,
      SampleLines: sa.samples,           // empty when patterns are used
      Patterns:    sa.patterns,
    }
```

The `bucket` struct gains a `template` field, set from `LogLine.PatternTemplate`
when the first line of a new pattern arrives.

**Edge case — PatternID changes mid-window due to merge:** If Drain merges
two templates while the aggregation window is open, lines stamped with
the old PatternID are already in a bucket under that ID. New lines get the
new ID. This results in the flushed Alert having two PatternSummary entries
for what is logically the same pattern. This is acceptable — at most a
one-window miscount, and it self-corrects in the next window.

---

## 4. Changes to Notifiers

### 4.1 LogNotifier

Print pattern details:
```
INFO ALERT service=notifications-processor level=ERROR count=34 window=1m0s patterns=2
  [28x ERROR] EventSubtypes.UpdateStatus fail: Operation cannot be fulfilled on <*> "<*>": the object has been modified
  [ 6x ERROR] EventSubtypes.Update fail: <*> is invalid: [<*>]
```

### 4.2 SlackNotifier

Each pattern gets its own Block Kit section:
```
:rotating_light: Error Alert: notifications-processor
34 errors in 1m (2 patterns)

Pattern 1 (28x, ERROR):
`EventSubtypes.UpdateStatus fail: Operation cannot be fulfilled on <*> "<*>": the object has been modified`
> sample: {...raw log...}

Pattern 2 (6x, ERROR):
`EventSubtypes.Update fail: <*> is invalid: [<*>]`
> sample: {...raw log...}
```

---

## 5. Configuration

```yaml
# config.yaml additions
pattern:
  enabled: true            # set false to skip L2 entirely (Phase 1 behavior)
  depth: 4                 # Drain tree depth
  similarity: 0.5          # merge threshold (0.0–1.0)
  max_children: 100        # max children per tree node
  max_patterns: 10000      # total patterns before LRU eviction
  extract_json_message: true  # extract msg/message/err from JSON before Drain
```

When `pattern.enabled` is false, the pipeline skips PatternEngine entirely
and works exactly like Phase 1.

---

## 6. Pipeline Wiring (main.go)

```go
// After
filtered := ingest.Filter(ctx, logCh)

var pipelined <-chan ingest.LogLine
if cfg.Pattern.Enabled {
    engine := pattern.NewPatternEngine(pattern.DrainConfig{
        Depth:               cfg.Pattern.Depth,
        SimilarityThreshold: cfg.Pattern.Similarity,
        MaxChildren:         cfg.Pattern.MaxChildren,
        MaxPatterns:         cfg.Pattern.MaxPatterns,
    })
    pipelined = engine.Run(ctx, filtered)
} else {
    pipelined = filtered
}

alerts := aggregator.Run(ctx, pipelined)
```

---

## 7. Design Decisions & Tradeoffs

### D1: Separate `pattern` package vs. extending `ingest`

**Decision:** New package `internal/pattern`.

**Rationale:** Pattern detection is a reusable concern — L3 will query the
pattern catalog to compute baselines. Keeping it separate avoids making
`ingest` a kitchen-sink package and prevents import cycles when `notify`
needs pattern types.

### D2: Pre-process JSON logs before Drain

**Decision:** Extract message fields from JSON logs.

**Rationale:** Without this, JSON keys, timestamps, and structure tokens
dominate the token vector, causing Drain to create too many patterns or
merge unrelated templates. The notifications-processor logs are 100% JSON.

**Risk:** If a service has important distinguishing info only in non-message
fields (e.g. a `method` field), those differences get lost. Mitigation:
we extract `msg` AND `err` AND `method` fields — the most useful ones.

### D3: PatternID on LogLine vs. separate channel type

**Decision:** Add `PatternID` field to `LogLine`.

**Rationale:** Keeps the pipeline type uniform — every stage consumes and
produces `<-chan LogLine`. A separate `PatternedLine` type would require
a new Aggregator signature and break the clean channel chain.

### D4: LRU eviction for max patterns

**Decision:** Evict least-recently-matched patterns when `maxPatterns`
is reached.

**Rationale:** In a long-running agent, some patterns appear during
deploys or transient incidents and never recur. Without eviction, memory
grows without bound. LRU ensures the working set stays relevant.

**Implementation:** Each `Pattern` carries a `LastMatched time.Time` field,
updated on every match. When `maxPatterns` is exceeded, scan all patterns
and evict the one with the oldest `LastMatched`. This is O(n) but
n ≤ 10,000 and eviction is rare — simpler than a linked-list LRU.

**Alternative considered:** Time-based eviction (patterns not seen in 24h).
Rejected because it requires another clock dependency and timer goroutine.
LRU is simpler and achieves the same goal. L3 can add time-based eviction
later when it has its own persistence.

### D5: Single goroutine for PatternEngine

**Decision:** No concurrency inside PatternEngine.

**Rationale:** The Drain tree is mutable shared state. Adding a mutex would
add overhead for every line. Since pattern matching is CPU-bound (tokenize +
tree walk + similarity), and the bottleneck is the Loki poll interval
(30s between 5000-line batches), a single goroutine easily keeps up.
Benchmarks in Phase 1 show ParseLevel at ~1μs/line; Drain should be
similar, putting 5000 lines at ~5ms — trivial.

### D6: Aggregator flush produces per-service alerts

**Decision:** Each Alert covers one service, with all its patterns inlined.

**Rationale:** Operators think in terms of services, not patterns. A single
alert per service with a pattern breakdown is more actionable than N
separate alerts (one per pattern). It also keeps Slack manageable — one
message per service per window.

---

## 8. File Layout

```
internal/pattern/
├── preprocess.go       # JSON extraction + regex normalization
├── drain.go            # Drain tree + Pattern type
├── engine.go           # PatternEngine channel stage
├── preprocess_test.go  # Unit tests for preprocessing
├── drain_test.go       # Unit tests for Drain algorithm
├── engine_test.go      # Channel stage tests
└── bench_test.go       # Benchmarks
```

Modified files:
```
internal/ingest/source.go      # Add PatternID to LogLine
internal/notify/notifier.go    # Add PatternSummary, modify Alert
internal/notify/aggregator.go  # Group by (service, patternID), build Patterns
internal/notify/slack.go       # Render pattern details
internal/notify/log.go         # Print pattern details
cmd/agent/main.go              # Wire PatternEngine, add pattern config
config/config.yaml             # Add pattern section
```
