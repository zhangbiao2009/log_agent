package alert

import (
	"context"
	"testing"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/core"
	"github.com/zhangbiao2009/log_agent/internal/ingest"
	"github.com/zhangbiao2009/log_agent/internal/testutil"
)

// sendAndDrain sends lines and waits briefly for goroutine to process.
func sendAndDrain(in chan<- ingest.LogLine, lines []ingest.LogLine) {
	for _, l := range lines {
		in <- l
	}
	// Give the goroutine time to pull from the buffered channel
	time.Sleep(50 * time.Millisecond)
}

func TestAggregator_BasicWindow(t *testing.T) {
	clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	agg := &Aggregator{
		Window:   1 * time.Minute,
		MinCount: 1,
		Clock:    clock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 10)
	out := agg.Run(ctx, in)

	sendAndDrain(in, []ingest.LogLine{
		{Service: "svc1", Level: "ERROR", Raw: "error 1"},
		{Service: "svc1", Level: "ERROR", Raw: "error 2"},
		{Service: "svc2", Level: "WARN", Raw: "warn 1"},
	})

	clock.Advance(1*time.Minute + time.Millisecond)

	var alerts []core.Alert
	timeout := time.After(2 * time.Second)
	for len(alerts) < 2 {
		select {
		case a := <-out:
			alerts = append(alerts, a)
		case <-timeout:
			t.Fatalf("timeout: expected 2 alerts, got %d: %+v", len(alerts), alerts)
		}
	}

	var svc1, svc2 *core.Alert
	for i := range alerts {
		switch alerts[i].Service {
		case "svc1":
			svc1 = &alerts[i]
		case "svc2":
			svc2 = &alerts[i]
		}
	}

	if svc1 == nil || svc2 == nil {
		t.Fatalf("missing alerts: svc1=%v, svc2=%v", svc1, svc2)
	}
	if svc1.Count != 2 {
		t.Errorf("svc1 count = %d, want 2", svc1.Count)
	}
	if svc1.Level != "ERROR" {
		t.Errorf("svc1 level = %s, want ERROR", svc1.Level)
	}
	if svc2.Count != 1 {
		t.Errorf("svc2 count = %d, want 1", svc2.Count)
	}
}

func TestAggregator_SeverityRanking(t *testing.T) {
	clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	agg := &Aggregator{
		Window:   1 * time.Minute,
		MinCount: 1,
		Clock:    clock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 10)
	out := agg.Run(ctx, in)

	sendAndDrain(in, []ingest.LogLine{
		{Service: "svc1", Level: "WARN", Raw: "warn"},
		{Service: "svc1", Level: "ERROR", Raw: "error"},
		{Service: "svc1", Level: "FATAL", Raw: "fatal"},
	})

	clock.Advance(1*time.Minute + time.Millisecond)

	select {
	case a := <-out:
		if a.Level != "FATAL" {
			t.Errorf("level = %s, want FATAL (highest)", a.Level)
		}
		if a.Count != 3 {
			t.Errorf("count = %d, want 3", a.Count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for alert")
	}
}

func TestAggregator_MinCountThreshold(t *testing.T) {
	clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	agg := &Aggregator{
		Window:   1 * time.Minute,
		MinCount: 5,
		Clock:    clock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 10)
	out := agg.Run(ctx, in)

	sendAndDrain(in, []ingest.LogLine{
		{Service: "svc1", Level: "ERROR", Raw: "err"},
		{Service: "svc1", Level: "ERROR", Raw: "err"},
		{Service: "svc1", Level: "ERROR", Raw: "err"},
	})

	clock.Advance(1*time.Minute + time.Millisecond)

	select {
	case a := <-out:
		t.Errorf("unexpected alert: %+v", a)
	case <-time.After(200 * time.Millisecond):
		// Expected: no alert below threshold
	}
}

func TestAggregator_SamplesCappedAt5(t *testing.T) {
	clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	agg := &Aggregator{
		Window:   1 * time.Minute,
		MinCount: 1,
		Clock:    clock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 20)
	out := agg.Run(ctx, in)

	var lines []ingest.LogLine
	for i := 0; i < 10; i++ {
		lines = append(lines, ingest.LogLine{Service: "svc1", Level: "ERROR", Raw: "err line"})
	}
	sendAndDrain(in, lines)

	clock.Advance(1*time.Minute + time.Millisecond)

	select {
	case a := <-out:
		if len(a.SampleLines) != 5 {
			t.Errorf("sample lines = %d, want 5", len(a.SampleLines))
		}
		if a.Count != 10 {
			t.Errorf("count = %d, want 10", a.Count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for alert")
	}
}

func TestAggregator_FlushOnClose(t *testing.T) {
	clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	agg := &Aggregator{
		Window:   1 * time.Minute,
		MinCount: 1,
		Clock:    clock,
	}

	ctx := context.Background()
	in := make(chan ingest.LogLine, 10)
	out := agg.Run(ctx, in)

	in <- ingest.LogLine{Service: "svc1", Level: "ERROR", Raw: "err"}
	time.Sleep(50 * time.Millisecond)
	close(in)

	select {
	case a := <-out:
		if a.Service != "svc1" {
			t.Errorf("service = %s, want svc1", a.Service)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for flush-on-close alert")
	}
}

func TestAggregator_PatternGrouping(t *testing.T) {
	clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	agg := &Aggregator{
		Window:   1 * time.Minute,
		MinCount: 1,
		Clock:    clock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 20)
	out := agg.Run(ctx, in)

	// Two distinct patterns for the same service.
	sendAndDrain(in, []ingest.LogLine{
		{Service: "svc", Level: "ERROR", Raw: "timeout", PatternID: "pat1", PatternTemplate: "timeout"},
		{Service: "svc", Level: "ERROR", Raw: "timeout", PatternID: "pat1", PatternTemplate: "timeout"},
		{Service: "svc", Level: "WARN", Raw: "disk full", PatternID: "pat2", PatternTemplate: "disk full"},
	})

	clock.Advance(1*time.Minute + time.Millisecond)

	select {
	case a := <-out:
		if a.Service != "svc" {
			t.Errorf("service = %s, want svc", a.Service)
		}
		if a.Count != 3 {
			t.Errorf("count = %d, want 3", a.Count)
		}
		if len(a.Patterns) != 2 {
			t.Errorf("pattern count = %d, want 2", len(a.Patterns))
		}
		// Patterns should be sorted by count descending.
		if a.Patterns[0].Count < a.Patterns[1].Count {
			t.Errorf("patterns not sorted by count desc: %+v", a.Patterns)
		}
		// pat1 has count 2, pat2 has count 1, so pat1 should be first.
		if a.Patterns[0].Template != "timeout" {
			t.Errorf("first pattern template = %q, want %q", a.Patterns[0].Template, "timeout")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for alert")
	}
}

func TestAggregator_NoPatternIDFallsBackToSamples(t *testing.T) {
	clock := testutil.NewFakeClock(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC))
	agg := &Aggregator{
		Window:   1 * time.Minute,
		MinCount: 1,
		Clock:    clock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingest.LogLine, 10)
	out := agg.Run(ctx, in)

	// Lines WITHOUT PatternID (pattern engine disabled).
	sendAndDrain(in, []ingest.LogLine{
		{Service: "svc", Level: "ERROR", Raw: "error line 1"},
		{Service: "svc", Level: "ERROR", Raw: "error line 2"},
	})

	clock.Advance(1*time.Minute + time.Millisecond)

	select {
	case a := <-out:
		// No patterns when PatternID is empty.
		if len(a.Patterns) != 0 {
			t.Errorf("expected no patterns for lines without PatternID, got %d", len(a.Patterns))
		}
		// Should have sample lines.
		if len(a.SampleLines) == 0 {
			t.Error("expected sample lines as fallback")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for alert")
	}
}
