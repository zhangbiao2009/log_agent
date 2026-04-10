package ingest

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
)

var (
	ansiRegex         = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	kvLevelRegex      = regexp.MustCompile(`(?:^|\s)level="?(\w+)"?(?:\s|$)`)
	bracketLevelRegex = regexp.MustCompile(`\[(\w+)\]`)
)

var levelNames = map[string]string{
	"error":   "ERROR",
	"err":     "ERROR",
	"fatal":   "FATAL",
	"warn":    "WARN",
	"warning": "WARN",
	"info":    "",
	"inf":     "",
	"debug":   "",
	"dbg":     "",
	"trace":   "",
}

var errorLevelKeywords = []struct {
	keyword string
	level   string
}{
	{"FATAL", "FATAL"},
	{"panic", "FATAL"},
	{"ERROR", "ERROR"},
	{"WARN", "WARN"},
}

func ParseLevel(raw string) string {
	// PERF: ansiRegex.ReplaceAllString runs on every line even when most lines
	// have no ANSI codes. A fast path check for the ESC byte (0x1b) before
	// invoking the regex would skip the allocation for the common case.
	cleaned := ansiRegex.ReplaceAllString(raw, "")

	if level, found := parseJSONLevel(cleaned); found {
		return level
	}
	if level, found := parseKVLevel(cleaned); found {
		return level
	}
	if level, found := parseBracketLevel(cleaned); found {
		return level
	}
	return scanKeywords(cleaned)
}

func parseJSONLevel(s string) (string, bool) {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return "", false
	}
	// PERF: json.Unmarshal into map[string]interface{} is the most expensive
	// parse path (~900ns, 26 allocs). We parse the entire JSON object just to
	// read one field. Two alternatives:
	//   1. Use a targeted string scan: find `"level":"` and extract the value
	//      without parsing the full object (~10x faster, 0 allocs).
	//   2. Unmarshal into a small struct with only the level fields, avoiding
	//      the interface{} boxing allocations.
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
		return "", false
	}
	for _, key := range []string{"level", "severity", "log_level"} {
		if val, ok := obj[key]; ok {
			if str, ok := val.(string); ok {
				return normalizeLevel(str), true
			}
		}
	}
	return "", false
}

func parseKVLevel(s string) (string, bool) {
	matches := kvLevelRegex.FindStringSubmatch(s)
	if matches == nil {
		return "", false
	}
	return normalizeLevel(matches[1]), true
}

func parseBracketLevel(s string) (string, bool) {
	// PERF: FindAllStringSubmatch allocates a [][]string for every bracket
	// pair in the line. A manual scan for '[' and ']' with a lookup into
	// levelNames would eliminate these allocations entirely.
	matches := bracketLevelRegex.FindAllStringSubmatch(s, -1)
	for _, m := range matches {
		word := strings.ToLower(m[1])
		if _, known := levelNames[word]; known {
			return normalizeLevel(m[1]), true
		}
	}
	return "", false
}

func scanKeywords(s string) string {
	for _, kw := range errorLevelKeywords {
		if strings.Contains(s, kw.keyword) {
			return kw.level
		}
	}
	return ""
}

func normalizeLevel(s string) string {
	if mapped, ok := levelNames[strings.ToLower(s)]; ok {
		return mapped
	}
	return ""
}

func Filter(ctx context.Context, in <-chan LogLine) <-chan LogLine {
	out := make(chan LogLine)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case line, ok := <-in:
				if !ok {
					return
				}
				level := ParseLevel(line.Raw)
				if level == "" {
					continue
				}
				line.Level = level
				select {
				case out <- line:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
