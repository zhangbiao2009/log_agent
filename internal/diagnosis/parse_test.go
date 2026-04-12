package diagnosis

import (
	"strings"
	"testing"
)

func TestParseDiagnosis_FullResponse(t *testing.T) {
	raw := `SEVERITY: P1
DIAGNOSIS: bank-gateway v2.3.1 is refusing connections.
SUGGESTIONS:
- Rollback bank-gateway to v2.3.0
- Check deployment logs
- Monitor after rollback`

	severity, diag, suggestions := ParseDiagnosis(raw)
	if severity != "P1" {
		t.Errorf("severity = %q, want P1", severity)
	}
	if !strings.Contains(diag, "bank-gateway") {
		t.Errorf("diagnosis should contain bank-gateway, got: %q", diag)
	}
	if len(suggestions) != 3 {
		t.Fatalf("len(suggestions) = %d, want 3", len(suggestions))
	}
	if !strings.Contains(suggestions[0], "Rollback") {
		t.Errorf("suggestions[0] = %q, want Rollback...", suggestions[0])
	}
}

func TestParseDiagnosis_P2Severity(t *testing.T) {
	raw := `SEVERITY: P2
DIAGNOSIS: Minor issue.
SUGGESTIONS:
- Fix it`
	severity, _, _ := ParseDiagnosis(raw)
	if severity != "P2" {
		t.Errorf("severity = %q, want P2", severity)
	}
}

func TestParseDiagnosis_P3Severity(t *testing.T) {
	raw := `SEVERITY: P3
DIAGNOSIS: Low priority.
SUGGESTIONS:
- Investigate later`
	severity, _, _ := ParseDiagnosis(raw)
	if severity != "P3" {
		t.Errorf("severity = %q, want P3", severity)
	}
}

func TestParseDiagnosis_MissingSeverity(t *testing.T) {
	raw := `DIAGNOSIS: Something went wrong.
SUGGESTIONS:
- action 1
- action 2`
	severity, diag, suggestions := ParseDiagnosis(raw)
	if severity != "P2" {
		t.Errorf("severity = %q, want P2 (default)", severity)
	}
	if !strings.Contains(diag, "Something went wrong") {
		t.Errorf("diagnosis = %q, want something went wrong", diag)
	}
	if len(suggestions) != 2 {
		t.Errorf("len(suggestions) = %d, want 2", len(suggestions))
	}
}

func TestParseDiagnosis_MissingDiagnosis(t *testing.T) {
	raw := `SEVERITY: P1
SUGGESTIONS:
- action 1`
	severity, diag, suggestions := ParseDiagnosis(raw)
	if severity != "P1" {
		t.Errorf("severity = %q, want P1", severity)
	}
	// No DIAGNOSIS: line → falls back to full raw text.
	if diag != raw {
		t.Errorf("diagnosis should be full raw text when DIAGNOSIS: line is missing")
	}
	if len(suggestions) != 1 {
		t.Errorf("len(suggestions) = %d, want 1", len(suggestions))
	}
}

func TestParseDiagnosis_MissingSuggestions(t *testing.T) {
	raw := `SEVERITY: P1
DIAGNOSIS: Root cause identified.`
	severity, diag, suggestions := ParseDiagnosis(raw)
	if severity != "P1" {
		t.Errorf("severity = %q, want P1", severity)
	}
	if !strings.Contains(diag, "Root cause identified") {
		t.Errorf("diagnosis = %q, want root cause text", diag)
	}
	if suggestions != nil {
		t.Errorf("suggestions = %v, want nil", suggestions)
	}
}

func TestParseDiagnosis_EmptyResponse(t *testing.T) {
	severity, diag, suggestions := ParseDiagnosis("")
	if severity != "P2" {
		t.Errorf("severity = %q, want P2", severity)
	}
	if diag != "" {
		t.Errorf("diagnosis = %q, want empty", diag)
	}
	if suggestions != nil {
		t.Errorf("suggestions = %v, want nil", suggestions)
	}
}

func TestParseDiagnosis_FreeformText(t *testing.T) {
	raw := "The database is down and everything is broken. Fix it now."
	severity, diag, suggestions := ParseDiagnosis(raw)
	if severity != "P2" {
		t.Errorf("severity = %q, want P2 (default)", severity)
	}
	if diag != raw {
		t.Errorf("expected full raw text as diagnosis fallback")
	}
	if suggestions != nil {
		t.Errorf("suggestions = %v, want nil", suggestions)
	}
}

func TestParseDiagnosis_InvalidSeverity(t *testing.T) {
	raw := `SEVERITY: CRITICAL
DIAGNOSIS: Something bad.`
	severity, _, _ := ParseDiagnosis(raw)
	if severity != "P2" {
		t.Errorf("severity = %q, want P2 (default for invalid severity)", severity)
	}
}

func TestParseDiagnosis_MultiLineDiagnosis(t *testing.T) {
	raw := `SEVERITY: P1
DIAGNOSIS: Line one of the diagnosis.
This continues on the next line.
And another line.
SUGGESTIONS:
- Fix it`
	severity, diag, suggestions := ParseDiagnosis(raw)
	if severity != "P1" {
		t.Errorf("severity = %q, want P1", severity)
	}
	if !strings.Contains(diag, "Line one") {
		t.Errorf("diagnosis missing first line: %q", diag)
	}
	if !strings.Contains(diag, "continues") {
		t.Errorf("diagnosis missing continuation: %q", diag)
	}
	if !strings.Contains(diag, "another line") {
		t.Errorf("diagnosis missing third line: %q", diag)
	}
	if len(suggestions) != 1 {
		t.Errorf("len(suggestions) = %d, want 1", len(suggestions))
	}
}
