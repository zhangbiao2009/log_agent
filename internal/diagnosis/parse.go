package diagnosis

import (
	"strings"
)

// ParseDiagnosis extracts severity, diagnosis, and suggestions from a
// structured LLM response. Handles malformed output gracefully.
func ParseDiagnosis(raw string) (severity, diagnosis string, suggestions []string) {
	severity = "P2" // default
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return severity, "", nil
	}

	lines := strings.Split(raw, "\n")

	// Extract severity.
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "SEVERITY:") {
			val := strings.TrimSpace(strings.TrimPrefix(trimmed, "SEVERITY:"))
			switch val {
			case "P1", "P2", "P3":
				severity = val
			}
			break
		}
	}

	// Extract diagnosis: text after "DIAGNOSIS:" until "SUGGESTIONS:" or end.
	diagStart := -1
	diagEnd := len(lines)
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "DIAGNOSIS:") {
			diagStart = i
		}
		if strings.HasPrefix(trimmed, "SUGGESTIONS:") && diagStart >= 0 {
			diagEnd = i
			break
		}
	}

	if diagStart >= 0 {
		// First line: text after "DIAGNOSIS:"
		firstLine := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(lines[diagStart]), "DIAGNOSIS:"))
		var diagParts []string
		if firstLine != "" {
			diagParts = append(diagParts, firstLine)
		}
		// Subsequent lines until SUGGESTIONS: or end.
		for i := diagStart + 1; i < diagEnd; i++ {
			trimmed := strings.TrimSpace(lines[i])
			if strings.HasPrefix(trimmed, "SEVERITY:") {
				continue
			}
			if trimmed != "" {
				diagParts = append(diagParts, trimmed)
			}
		}
		diagnosis = strings.Join(diagParts, "\n")
	} else {
		// No DIAGNOSIS: prefix — use full raw text as diagnosis.
		diagnosis = raw
	}

	// Extract suggestions: lines starting with "- " after "SUGGESTIONS:".
	sugStart := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "SUGGESTIONS:") {
			sugStart = i + 1
			break
		}
	}
	if sugStart >= 0 {
		for i := sugStart; i < len(lines); i++ {
			trimmed := strings.TrimSpace(lines[i])
			if strings.HasPrefix(trimmed, "- ") {
				suggestions = append(suggestions, strings.TrimPrefix(trimmed, "- "))
			}
		}
	}

	return severity, diagnosis, suggestions
}
