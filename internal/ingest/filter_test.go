package ingest

import (
	"context"
	"testing"
	"time"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// --- JSON structured ---
		{
			name:  "json level error",
			input: `{"level":"error","message":"connection refused","ts":"2025-01-01T00:00:00Z"}`,
			want:  "ERROR",
		},
		{
			name:  "json severity fatal",
			input: `{"severity":"fatal","msg":"OOM killed"}`,
			want:  "FATAL",
		},
		{
			name:  "json level warn",
			input: `{"level":"warning","message":"slow query"}`,
			want:  "WARN",
		},
		{
			name:  "json debug with ERROR in msg - structured authoritative",
			input: `{"level":"debug","message":"handling ERROR code path"}`,
			want:  "",
		},
		{
			name:  "json info level returns empty",
			input: `{"level":"info","message":"started"}`,
			want:  "",
		},
		// --- KV structured ---
		{
			name:  "kv level=error",
			input: `ts=2025-01-01 level=error msg="disk full"`,
			want:  "ERROR",
		},
		{
			name:  "kv level=debug with ERROR in msg - authoritative",
			input: `ts=2025-01-01 level=debug msg="ERROR count reset"`,
			want:  "",
		},
		{
			name:  "kv quoted level",
			input: `ts=2025-01-01 level="warn" msg="high latency"`,
			want:  "WARN",
		},
		// --- Bracket structured ---
		{
			name:  "bracket ERROR",
			input: `2025-01-01 10:00:00 [ERROR] connection timeout`,
			want:  "ERROR",
		},
		{
			name:  "bracket WARN",
			input: `2025-01-01 10:00:00 [WARN] retrying`,
			want:  "WARN",
		},
		{
			name:  "bracket debug with ERROR in body - authoritative",
			input: `2025-01-01 10:00:00 [debug] user saw ERROR page`,
			want:  "",
		},
		{
			name:  "bracket INFO returns empty",
			input: `2025-01-01 10:00:00 [INFO] started server`,
			want:  "",
		},
		// --- Keyword fallback ---
		{
			name:  "keyword ERROR in unstructured log",
			input: `Jan 01 10:00:00 myapp ERROR: connection refused`,
			want:  "ERROR",
		},
		{
			name:  "keyword FATAL in unstructured log",
			input: `Jan 01 10:00:00 myapp FATAL: out of memory`,
			want:  "FATAL",
		},
		{
			name:  "keyword WARN in unstructured log",
			input: `Jan 01 10:00:00 myapp WARN: high latency`,
			want:  "WARN",
		},
		{
			name:  "keyword panic",
			input: `goroutine 1 [running]: panic: runtime error`,
			want:  "FATAL",
		},
		// --- ANSI codes ---
		{
			name:  "ansi color codes stripped",
			input: "\x1b[31mERROR\x1b[0m connection refused",
			want:  "ERROR",
		},
		// --- Edge cases ---
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "no level info",
			input: `just a normal log line with some text`,
			want:  "",
		},
		{
			name:  "error in url not matched as structured",
			input: `2025-01-01 [INFO] fetching https://api.example.com/errors`,
			want:  "",
		},
		{
			name:  "multiple brackets picks first known level",
			input: `[INFO] connection to [ERROR] handler established`,
			want:  "",
		},
		// --- Abbreviations ---
		{
			name:  "kv level=err",
			input: `level=err msg="disk failure"`,
			want:  "ERROR",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseLevel(tc.input)
			if got != tc.want {
				t.Errorf("ParseLevel(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan LogLine, 5)
	now := time.Now()
	in <- LogLine{Service: "svc1", Timestamp: now, Raw: `{"level":"error","msg":"bad"}`}
	in <- LogLine{Service: "svc1", Timestamp: now, Raw: `{"level":"info","msg":"ok"}`}
	in <- LogLine{Service: "svc2", Timestamp: now, Raw: `FATAL: crash`}
	in <- LogLine{Service: "svc2", Timestamp: now, Raw: `just a normal line`}
	in <- LogLine{Service: "svc3", Timestamp: now, Raw: `{"level":"debug","msg":"handling ERROR"}`}
	close(in)

	out := Filter(ctx, in)
	var results []LogLine
	for line := range out {
		results = append(results, line)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 lines, got %d: %+v", len(results), results)
	}
	if results[0].Level != "ERROR" {
		t.Errorf("line 0: want ERROR, got %s", results[0].Level)
	}
	if results[1].Level != "FATAL" {
		t.Errorf("line 1: want FATAL, got %s", results[1].Level)
	}
}

func TestFilterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan LogLine, 1)
	out := Filter(ctx, in)
	cancel()

	// Drain should complete quickly after cancel
	for range out {
	}
}
