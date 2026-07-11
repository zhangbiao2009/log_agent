package alert

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/ingest"
	"github.com/zhangbiao2009/log_agent/internal/testutil"
)

func BenchmarkAggregator_Ingest(b *testing.B) {
	clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	agg := &Aggregator{
		Window:   1 * time.Hour, // large window so no flushes during bench
		MinCount: 1,
		Clock:    clock,
	}

	lines := []ingest.LogLine{
		{Service: "svc-1", Level: "ERROR", Raw: "error in handler"},
		{Service: "svc-2", Level: "WARN", Raw: "high latency warning"},
		{Service: "svc-1", Level: "FATAL", Raw: "OOM killed"},
		{Service: "svc-3", Level: "ERROR", Raw: "connection refused"},
		{Service: "svc-2", Level: "ERROR", Raw: "timeout"},
	}

	in := make(chan ingest.LogLine, 1000)
	ctx := contextBackground()
	out := agg.Run(ctx, in)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in <- lines[i%len(lines)]
	}
	close(in)

	// Drain output
	for range out {
	}
}

func BenchmarkAggregator_FlushManyServices(b *testing.B) {
	for b.Loop() {
		clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
		agg := &Aggregator{
			Window:   1 * time.Minute,
			MinCount: 1,
			Clock:    clock,
		}

		in := make(chan ingest.LogLine, 200)
		ctx := contextBackground()
		out := agg.Run(ctx, in)

		// 100 different services, 1 error each
		for i := 0; i < 100; i++ {
			in <- ingest.LogLine{
				Service: fmt.Sprintf("svc-%d", i),
				Level:   "ERROR",
				Raw:     "error message",
			}
		}
		close(in)

		for range out {
		}
	}
}

func contextBackground() context.Context {
	return context.Background()
}
