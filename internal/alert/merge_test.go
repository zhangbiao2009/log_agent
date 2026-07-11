package alert

import (
	"context"
	"testing"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/core"
)

// TestMergeAlerts_CombinesAllInputs verifies every alert from every input
// channel appears exactly once on the merged output.
func TestMergeAlerts_CombinesAllInputs(t *testing.T) {
	ctx := context.Background()

	in1 := make(chan core.Alert, 3)
	in2 := make(chan core.Alert, 3)

	in1 <- core.Alert{Service: "svc-a", Count: 1}
	in1 <- core.Alert{Service: "svc-a", Count: 2}
	in2 <- core.Alert{Service: "svc-b", Count: 3}
	close(in1)
	close(in2)

	out := MergeAlerts(ctx, in1, in2)

	counts := map[string]int{}
	total := 0
	for a := range out {
		counts[a.Service]++
		total++
	}

	if total != 3 {
		t.Fatalf("got %d alerts, want 3", total)
	}
	if counts["svc-a"] != 2 {
		t.Errorf("svc-a: got %d, want 2", counts["svc-a"])
	}
	if counts["svc-b"] != 1 {
		t.Errorf("svc-b: got %d, want 1", counts["svc-b"])
	}
}

// TestMergeAlerts_ClosesWhenAllInputsClose verifies the merged channel closes
// only after every input channel is drained and closed.
func TestMergeAlerts_ClosesWhenAllInputsClose(t *testing.T) {
	ctx := context.Background()
	in1 := make(chan core.Alert)
	in2 := make(chan core.Alert)
	out := MergeAlerts(ctx, in1, in2)

	close(in1)
	// out must not close yet — in2 still open.
	select {
	case _, ok := <-out:
		if !ok {
			t.Fatal("merged channel closed before all inputs closed")
		}
	case <-time.After(50 * time.Millisecond):
		// expected: no close, no data
	}

	close(in2)
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected merged channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("merged channel did not close after all inputs closed")
	}
}

// TestMergeAlerts_ContextCancelStops verifies cancelling ctx tears down the
// merge goroutines and closes the output even while inputs stay open.
func TestMergeAlerts_ContextCancelStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan core.Alert) // never closed
	out := MergeAlerts(ctx, in)

	cancel()

	select {
	case _, ok := <-out:
		if ok {
			// A value may race through; drain until closed.
			for range out {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("merged channel did not close after context cancel")
	}
}

// TestMergeAlerts_NoInputs verifies the zero-input case closes immediately.
func TestMergeAlerts_NoInputs(t *testing.T) {
	out := MergeAlerts(context.Background())
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected empty merged channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("empty merge did not close")
	}
}
