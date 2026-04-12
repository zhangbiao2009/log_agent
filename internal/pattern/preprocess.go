package pattern

import (
	"encoding/json"
	"regexp"
	"strings"
)

var (
	// uuidRe matches standard UUID v1-v5 format: 8-4-4-4-12 hex chars.
	uuidRe = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	// ipv4Re matches dotted-decimal IPv4 addresses.
	ipv4Re = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)
	// hexRe matches standalone hex strings of 8+ chars (e.g. span IDs, hashes).
	hexRe = regexp.MustCompile(`(?i)\b[0-9a-f]{8,}\b`)
	// numRe matches standalone decimal integers (not already replaced).
	numRe = regexp.MustCompile(`\b\d+\b`)
)

// Preprocess normalizes a raw log line for Drain tokenization.
// If the line is JSON, it extracts the human-readable message fields
// (msg/message, then method, then err/error) and joins them.
// It then replaces common variable tokens (UUIDs, IPs, hex strings, numbers)
// with placeholders so that structurally identical log lines produce the same
// token sequence.
func Preprocess(raw string) string {
	s := extractJSON(raw)
	// Apply replacements in order: UUID before IP (to avoid partial replacement),
	// then HEX (catches remaining hex literals), then NUM.
	s = uuidRe.ReplaceAllString(s, "<UUID>")
	s = ipv4Re.ReplaceAllString(s, "<IP>")
	s = hexRe.ReplaceAllString(s, "<HEX>")
	s = numRe.ReplaceAllString(s, "<NUM>")
	return s
}

// extractJSON attempts to parse raw as a JSON object and concatenate
// the string values of fields in priority order:
//  1. msg or message (first match wins)
//  2. method
//  3. err or error (first match wins)
//
// Returns raw unchanged if it is not a JSON object or no target fields are found.
func extractJSON(raw string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return raw
	}

	parts := make([]string, 0, 3)

	// 1. msg / message
	for _, key := range []string{"msg", "message"} {
		if v, ok := m[key]; ok {
			var s string
			if json.Unmarshal(v, &s) == nil && s != "" {
				parts = append(parts, s)
				break
			}
		}
	}

	// 2. method
	if v, ok := m["method"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil && s != "" {
			parts = append(parts, s)
		}
	}

	// 3. err / error
	for _, key := range []string{"err", "error"} {
		if v, ok := m[key]; ok {
			var s string
			if json.Unmarshal(v, &s) == nil && s != "" {
				parts = append(parts, s)
				break
			}
		}
	}

	if len(parts) == 0 {
		return raw
	}
	return strings.Join(parts, " ")
}
