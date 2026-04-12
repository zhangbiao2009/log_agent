# Phase 5: LLM Diagnosis — Test Plan

**Scope:** Unit tests for `internal/diagnosis` (prompt, parsing, pipeline,
HTTP client), updated tests for `internal/notify` (diagnosis rendering),
and an integration smoke test with a mock LLM.

---

## 1. Guiding Principles

- **No real LLM calls.** All tests use a `MockLLM` that returns canned
  responses. No network calls, no API keys, no flaky tests.
- **Prompt tests are structural.** Assert that the prompt contains required
  sections and data — not exact wording. This avoids brittle tests that
  break on cosmetic prompt changes.
- **Parse tests cover malformed input.** The LLM can return anything.
  Every parse test has a "happy path" and multiple "degenerate output"
  variants. The parser must never panic.
- **Fallback behavior is first-class.** LLM failure must produce a
  usable (if degraded) incident. Test the fallback path as thoroughly
  as the happy path.
- **Backward compatibility.** When the diagnoser is disabled (zero-value
  Diagnosis fields), all notifier output must match Phase 4 exactly.
  Existing Phase 4 tests must pass unchanged.
- **Race detector.** All tests run with `-race`.

---

## 2. Test Files

```
internal/diagnosis/
    prompt_test.go     — prompt assembly tests (9 cases)
    parse_test.go      — response parsing tests (10 cases)
    diagnoser_test.go  — pipeline stage tests (10 cases)
    llm_test.go        — HTTPClient tests with httptest (8 cases)
internal/notify/
    log_test.go        — (append) diagnosis rendering tests (3 cases)
    slack_test.go      — (append) diagnosis rendering tests (3 cases)
```

**Total: 43 test cases.**

---

## 3. `prompt_test.go` — Prompt Assembly (9 cases)

### 3.1 Basic Structure (3 cases)

**`TestBuildPrompt_ContainsSystemInstruction`**
- Input: incident with 1 service, 1 alert.
- Expected: prompt starts with "You are an SRE assistant".
- Rationale: system instruction is always present.

**`TestBuildPrompt_ContainsServiceList`**
- Input: incident with services `["order-svc", "payment-svc"]`.
- Expected: prompt contains "order-svc" and "payment-svc" in the
  "Affected services" line.
- Rationale: all services appear in context.

**`TestBuildPrompt_ContainsDepChain`**
- Input: incident with DepChain `["bank-gw", "payment-svc", "order-svc"]`.
- Expected: prompt contains "bank-gw → payment-svc → order-svc" (or
  equivalent joined representation).
- Rationale: dependency chain helps LLM reason about causation.

### 3.2 Alert Content (3 cases)

**`TestBuildPrompt_ContainsPatternTemplates`**
- Input: alert with 2 patterns — "connection refused to <*>:<*>"
  and "timeout calling <*>".
- Expected: prompt contains both template strings.
- Rationale: log patterns are the primary evidence for diagnosis.

**`TestBuildPrompt_ContainsSampleLines`**
- Input: alert with pattern containing 3 sample lines.
- Expected: prompt contains all 3 sample lines (quoted).
- Rationale: samples give the LLM concrete evidence.

**`TestBuildPrompt_ContainsAnomalyTags`**
- Input: alert with one SPIKE pattern (ZScore=5.2) and one NEW pattern.
- Expected: prompt contains "SPIKE" and "NEW" annotations near
  respective patterns.
- Rationale: anomaly context guides the LLM's attention.

### 3.3 Edge Cases (3 cases)

**`TestBuildPrompt_SingleAlertNoDepChain`**
- Input: single-alert incident (`IsSingleAlert() == true`), no dep chain.
- Expected: prompt omits "Dependency chain" line. "Suspected root cause"
  line is omitted or says "N/A".
- Rationale: single-service incidents have no chain to display.

**`TestBuildPrompt_EmptyPatterns`**
- Input: alert with `Patterns == nil` (edge case from bypass).
- Expected: prompt renders service section with count and level but
  no pattern subsections. No panic.
- Rationale: defensive against missing pattern data.

**`TestBuildPrompt_TruncatesLargeIncidents`**
- Input: incident with 12 services, each with 5 patterns.
- Expected: prompt includes at most 5 service sections (the ones
  with highest anomaly scores). Prompt contains a note like
  "(7 additional services omitted)".
- Rationale: prevent prompt from exceeding token budget.

---

## 4. `parse_test.go` — Response Parsing (10 cases)

### 4.1 Happy Path (3 cases)

**`TestParseDiagnosis_FullResponse`**
- Input:
  ```
  SEVERITY: P1
  DIAGNOSIS: bank-gateway v2.3.1 is refusing connections.
  SUGGESTIONS:
  - Rollback bank-gateway to v2.3.0
  - Check deployment logs
  - Monitor after rollback
  ```
- Expected: severity=`"P1"`, diagnosis contains "bank-gateway",
  suggestions=`["Rollback...", "Check...", "Monitor..."]`.
- Rationale: standard structured response.

**`TestParseDiagnosis_P2Severity`**
- Input: response with `SEVERITY: P2`.
- Expected: severity=`"P2"`.
- Rationale: all severity levels are parsed.

**`TestParseDiagnosis_P3Severity`**
- Input: response with `SEVERITY: P3`.
- Expected: severity=`"P3"`.
- Rationale: all severity levels are parsed.

### 4.2 Missing Sections (3 cases)

**`TestParseDiagnosis_MissingSeverity`**
- Input: response with DIAGNOSIS and SUGGESTIONS but no SEVERITY line.
- Expected: severity=`"P2"` (default), diagnosis and suggestions
  extracted normally.
- Rationale: graceful degradation.

**`TestParseDiagnosis_MissingDiagnosis`**
- Input: response has SEVERITY and SUGGESTIONS but no DIAGNOSIS line.
- Expected: diagnosis is the full raw response (fallback), severity
  and suggestions extracted normally.
- Rationale: use what we can.

**`TestParseDiagnosis_MissingSuggestions`**
- Input: response has SEVERITY and DIAGNOSIS but no SUGGESTIONS section.
- Expected: suggestions=`nil` (or empty slice), severity and diagnosis
  extracted normally.
- Rationale: suggestions are optional.

### 4.3 Malformed / Adversarial Input (4 cases)

**`TestParseDiagnosis_EmptyResponse`**
- Input: `""`.
- Expected: severity=`"P2"`, diagnosis=`""`, suggestions=`nil`.
- Rationale: empty LLM response must not panic.

**`TestParseDiagnosis_FreeformText`**
- Input: "The database is down and everything is broken. Fix it now."
  (no structured sections).
- Expected: severity=`"P2"` (default), diagnosis = full raw text,
  suggestions=`nil`.
- Rationale: unstructured response is used as-is for diagnosis.

**`TestParseDiagnosis_InvalidSeverity`**
- Input: `SEVERITY: CRITICAL` (not P1/P2/P3).
- Expected: severity=`"P2"` (default fallback).
- Rationale: unexpected severity values don't propagate.

**`TestParseDiagnosis_MultiLineDiagnosis`**
- Input:
  ```
  SEVERITY: P1
  DIAGNOSIS: Line one of the diagnosis.
  This continues on the next line.
  And another line.
  SUGGESTIONS:
  - Fix it
  ```
- Expected: diagnosis contains all three lines (joined or preserved
  with newlines). Suggestions still parsed.
- Rationale: multi-line diagnosis is common from LLMs.

---

## 5. `diagnoser_test.go` — Pipeline Stage (10 cases)

All diagnoser tests use:
- `MockLLM{Response: "...", Err: nil}` — returns canned response.
- `makeIncident(services...)` helper — constructs a minimal Incident
  with the given services, one alert per service.
- `collectIncidents(out, n, timeout)` — same pattern as correlator tests.

### 5.1 Pipeline Mechanics (3 cases)

**`TestDiagnoser_ClosesOutputWhenInputCloses`**
- Close input immediately (no incidents).
- Expected: output closes.
- Rationale: no goroutine leak.

**`TestDiagnoser_ClosesOutputOnContextCancel`**
- Send one incident, cancel ctx before LLM responds.
- MockLLM blocks until ctx is cancelled (use a channel-gated mock).
- Expected: output closes within 1s.
- Rationale: graceful shutdown; LLM call is cancelled.

**`TestDiagnoser_PassesThroughAllIncidents`**
- Send 3 incidents, MockLLM returns canned response for each.
- Expected: 3 enriched incidents on output, in order.
- Rationale: all incidents are processed sequentially.

### 5.2 Enrichment (3 cases)

**`TestDiagnoser_SetsDiagnosisField`**
- MockLLM response contains `DIAGNOSIS: root cause explanation`.
- Expected: output incident has `Diagnosis == "root cause explanation"`.
- Rationale: diagnosis is extracted and set.

**`TestDiagnoser_SetsSeverityField`**
- MockLLM response contains `SEVERITY: P1`.
- Expected: output incident has `Severity == "P1"`.
- Rationale: severity is extracted and set.

**`TestDiagnoser_SetsSuggestionsField`**
- MockLLM response contains 3 suggestion bullets.
- Expected: output incident has `len(Suggestions) == 3`.
- Rationale: suggestions are extracted and set.

### 5.3 Failure / Fallback (4 cases)

**`TestDiagnoser_LLMErrorFallbackDiagnosis`**
- MockLLM returns `Err: errors.New("connection refused")`.
- Expected: incident passes through with
  `Diagnosis` containing "LLM diagnosis unavailable",
  `Severity` set by heuristic (not empty).
  `Suggestions == nil`.
- Rationale: pipeline continues on LLM failure.

**`TestDiagnoser_LLMErrorPreservesExistingFields`**
- MockLLM returns error. Incident has Services, Alerts, RootService.
- Expected: all existing fields unchanged after diagnoser runs.
- Rationale: LLM failure must not corrupt the incident.

**`TestDiagnoser_HeuristicSeverityP1`**
- MockLLM returns error. Incident has 3+ services.
- Expected: `Severity == "P1"` (heuristic: ≥3 services → P1).
- Rationale: heuristic fallback produces reasonable severity.

**`TestDiagnoser_HeuristicSeverityP3`**
- MockLLM returns error. Incident has 1 service, no FATAL.
- Expected: `Severity == "P3"`.
- Rationale: single-service, non-critical → lowest severity.

---

## 6. `llm_test.go` — HTTPClient (8 cases)

All tests use `httptest.NewServer` to simulate the LLM API. No real
network calls.

### 6.1 Request Format (2 cases)

**`TestHTTPClient_RequestFormat`**
- Start httptest server that records the request.
- Call `Complete(ctx, "test prompt")`.
- Expected: request is POST to `/v1/chat/completions`, content-type
  is `application/json`, body contains `"model"`, `"messages"` array
  with role `"user"` and content `"test prompt"`, `"temperature": 0`,
  `"max_tokens"`.
- Rationale: verify OpenAI-compatible request format.

**`TestHTTPClient_AuthorizationHeader`**
- Start httptest server that records headers.
- Create client with API key "sk-test-123".
- Expected: request has `Authorization: Bearer sk-test-123`.
- Rationale: API key is sent as bearer token.

### 6.2 Response Handling (3 cases)

**`TestHTTPClient_SuccessfulResponse`**
- Server returns 200 with valid ChatCompletion JSON response.
- Expected: `Complete` returns the message content string, no error.
- Rationale: happy path response extraction.

**`TestHTTPClient_EmptyChoices`**
- Server returns 200 with `"choices": []`.
- Expected: returns error (no completion content).
- Rationale: edge case — valid JSON but no useful output.

**`TestHTTPClient_MalformedJSON`**
- Server returns 200 with body `"not json"`.
- Expected: returns error.
- Rationale: corrupted response handling.

### 6.3 Error Handling (3 cases)

**`TestHTTPClient_RateLimitRetry`**
- Server returns 429 on first call with `Retry-After: 1`, then 200.
- Expected: `Complete` succeeds (retried once), returns the response.
  Verify server received exactly 2 requests.
- Rationale: rate limit retry works.

**`TestHTTPClient_ServerErrorRetry`**
- Server returns 500 on first call, then 200.
- Expected: `Complete` succeeds after retry. Server received 2 requests.
- Rationale: transient server error triggers retry.

**`TestHTTPClient_ClientErrorNoRetry`**
- Server returns 400 with error body.
- Expected: `Complete` returns error immediately. Server received
  exactly 1 request (no retry on 4xx).
- Rationale: bad request is not retried.

---

## 7. Notify Tests — Diagnosis Rendering (6 cases)

### 7.1 `log_test.go` — Append 3 Cases

**`TestLogNotifier_IncidentWithDiagnosis`**
- Incident with `Severity="P1"`, `Diagnosis="root cause text"`,
  `Suggestions=["action 1", "action 2"]`.
- Expected: output contains "P1", "root cause text", "action 1",
  "action 2". All in readable format.
- Rationale: full diagnosis rendering.

**`TestLogNotifier_IncidentDiagnosisEmpty`**
- Incident with `Diagnosis=""` (diagnoser disabled).
- Expected: output matches Phase 4 format exactly — no diagnosis
  section, no severity line.
- Rationale: backward compatibility.

**`TestLogNotifier_IncidentSeverityOnly`**
- Incident with `Severity="P2"` but `Diagnosis=""` (heuristic fallback,
  LLM failed but severity was set).
- Expected: output contains "P2" severity but no diagnosis text section.
- Rationale: partial enrichment (severity without diagnosis) renders
  cleanly.

### 7.2 `slack_test.go` — Append 3 Cases

**`TestSlackNotifier_IncidentWithDiagnosisBlocks`**
- Incident with severity, diagnosis, and suggestions.
- Expected: Slack blocks include a diagnosis section block and a
  suggestions block. Header contains severity emoji. At least 4 blocks
  (header + diagnosis + suggestions + alert).
- Rationale: full Slack rendering.

**`TestSlackNotifier_IncidentDiagnosisEmptyBackwardCompat`**
- Incident with `Diagnosis=""`.
- Expected: Slack blocks match Phase 4 output — no diagnosis or
  suggestion blocks.
- Rationale: backward compatibility.

**`TestSlackNotifier_IncidentSuggestionsAsNumberedList`**
- Incident with 3 suggestions.
- Expected: suggestions block text contains "1.", "2.", "3." prefixed
  items.
- Rationale: suggestions are numbered in Slack for readability.

---

## 8. Integration Smoke Test

**File:** `internal/diagnosis/diagnoser_test.go`

**`TestDiagnoser_EndToEnd_MultiServiceIncident`**

Setup:
- MockLLM with realistic canned response:
  ```
  SEVERITY: P1
  DIAGNOSIS: bank-gateway v2.3.1 deployed at 14:30 is refusing connections
  on port 443. payment-service cannot reach bank-gateway, causing timeouts
  in order-service.
  SUGGESTIONS:
  - Rollback bank-gateway to v2.3.0
  - Check bank-gateway deployment logs for startup errors
  - Monitor error rates after rollback
  ```
- Incident: 3 services (bank-gw, payment-svc, order-svc),
  RootService="bank-gw", DepChain=["bank-gw", "payment-svc", "order-svc"],
  3 alerts with realistic patterns and sample lines.

Steps:
1. Send incident through diagnoser pipeline.
2. Collect output.

Assertions:
- Exactly 1 incident emitted.
- `Severity == "P1"`.
- `Diagnosis` contains "bank-gateway" and "refusing connections".
- `len(Suggestions) == 3`.
- `Suggestions[0]` contains "Rollback".
- All original fields preserved: `Services`, `RootService`, `DepChain`,
  `Alerts`, `ID`, `OpenedAt`, `Window` unchanged.
- MockLLM received exactly 1 call.
- The prompt passed to MockLLM contains all 3 service names and the
  dependency chain.

---

## 9. What We Do Not Test Here

| Concern | Reason omitted |
|---|---|
| Real LLM API calls | Flaky, costly; manual integration test only |
| RAG / past incident retrieval | Not in Phase 5 scope |
| Deploy context enrichment | Not in Phase 5 scope |
| Incident lifecycle transitions | Phase 6 |
| Notification deduplication | Phase 6 |
| Token counting / context truncation | Tested directionally via TruncatesLargeIncidents |
| Concurrent diagnoser instances | Single-goroutine by design |
| API key rotation | Operational concern, not unit-testable |

---

## 10. Test Execution Checklist

```bash
# New diagnosis tests
go test -race -v ./internal/diagnosis/...

# Updated notify rendering tests
go test -race -v ./internal/notify/...

# Full suite regression check (Phase 1-5)
go test -race ./...
```
