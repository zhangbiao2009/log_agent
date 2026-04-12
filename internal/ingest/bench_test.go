package ingest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// --- ParseLevel benchmarks: one per format path ---

func BenchmarkParseLevel_JSON(b *testing.B) {
	line := `{"level":"error","message":"connection refused","ts":"2025-01-01T00:00:00Z","caller":"main.go:42"}`
	for b.Loop() {
		ParseLevel(line)
	}
}

func BenchmarkParseLevel_KV(b *testing.B) {
	line := `ts=2025-01-01T10:00:00Z level=error caller=server.go:99 msg="connection refused"`
	for b.Loop() {
		ParseLevel(line)
	}
}

func BenchmarkParseLevel_Bracket(b *testing.B) {
	line := `2025-01-01 10:00:00.123 [ERROR] connection timeout after 30s`
	for b.Loop() {
		ParseLevel(line)
	}
}

func BenchmarkParseLevel_Keyword(b *testing.B) {
	line := `Jan 01 10:00:00 myapp ERROR: connection refused to database host`
	for b.Loop() {
		ParseLevel(line)
	}
}

func BenchmarkParseLevel_NoMatch(b *testing.B) {
	line := `2025-01-01 10:00:00 normal operational message with no level indicator whatsoever`
	for b.Loop() {
		ParseLevel(line)
	}
}

func BenchmarkParseLevel_ANSI(b *testing.B) {
	line := "\x1b[31mERROR\x1b[0m connection refused to host db-primary.internal:5432"
	for b.Loop() {
		ParseLevel(line)
	}
}

// --- Filter pipeline throughput ---

func BenchmarkFilterPipeline(b *testing.B) {
	now := time.Now()
	// Mix of error and non-error lines (roughly 20% error rate)
	lines := []LogLine{
		{Service: "svc1", Timestamp: now, Raw: `{"level":"error","msg":"bad request"}`},
		{Service: "svc1", Timestamp: now, Raw: `{"level":"info","msg":"request handled"}`},
		{Service: "svc1", Timestamp: now, Raw: `{"level":"info","msg":"health check ok"}`},
		{Service: "svc2", Timestamp: now, Raw: `2025-01-01 10:00:00 [INFO] starting`},
		{Service: "svc2", Timestamp: now, Raw: `2025-01-01 10:00:00 [ERROR] disk full`},
	}

	b.ResetTimer()
	for b.Loop() {
		ctx, cancel := context.WithCancel(context.Background())
		in := make(chan LogLine, len(lines))
		for _, l := range lines {
			in <- l
		}
		close(in)
		out := Filter(ctx, in)
		for range out {
		}
		cancel()
	}
}

// --- parseLokiResponse benchmark ---

func BenchmarkParseLokiResponse(b *testing.B) {
	// Build a realistic response with multiple streams and entries
	streams := ""
	for i := 0; i < 10; i++ {
		svc := fmt.Sprintf("service-%d", i)
		values := ""
		for j := 0; j < 100; j++ {
			ts := 1700000000000000000 + int64(i*1000+j)*1000000
			if j > 0 {
				values += ","
			}
			values += fmt.Sprintf(`["%d","level=error msg=\"request failed #%d\""]`, ts, j)
		}
		if i > 0 {
			streams += ","
		}
		streams += fmt.Sprintf(`{"stream":{"service":"%s","namespace":"prod"},"values":[%s]}`, svc, values)
	}
	body := fmt.Sprintf(`{"status":"success","data":{"resultType":"streams","result":[%s]}}`, streams)

	b.ResetTimer()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		r := strings.NewReader(body)
		parseLokiResponse(r, "")
	}
}
