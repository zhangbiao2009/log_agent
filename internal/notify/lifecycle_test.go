package notify

import (
	"context"
	"sync"
	"testing"
	"time"
)

// drainAsync starts reading from ch in a goroutine and returns
// the collected results once the channel closes or idleTimeout elapses
// with no new events.
func drainAsync(ch <-chan Incident, idleTimeout time.Duration) func() []Incident {
	var mu sync.Mutex
	var results []Incident
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			select {
			case inc, ok := <-ch:
				if !ok {
					return
				}
				mu.Lock()
				results = append(results, inc)
				mu.Unlock()
			case <-time.After(idleTimeout):
				return
			}
		}
	}()

	return func() []Incident {
		<-done
		mu.Lock()
		defer mu.Unlock()
		cp := make([]Incident, len(results))
		copy(cp, results)
		return cp
	}
}

func makeIncident(id string) Incident {
	return Incident{
		ID:       id,
		Services: []string{"svc-a"},
		Alerts:   []Alert{{Service: "svc-a", Level: "ERROR", Count: 5, Window: time.Minute}},
	}
}

func filterByEventType(incidents []Incident, eventType string) []Incident {
	var result []Incident
	for _, inc := range incidents {
		if inc.EventType == eventType {
			result = append(result, inc)
		}
	}
	return result
}

func eventTypes(incidents []Incident) []string {
	var types []string
	for _, inc := range incidents {
		types = append(types, inc.EventType+"("+inc.ID+")")
	}
	return types
}

// --- State Machine Tests ---

func TestLifecycle_NewIncident_EmitsOpened(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   5 * time.Minute,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	in := make(chan Incident, 1)
	in <- makeIncident("inc-1")
	close(in)

	out := lm.Run(context.Background(), in)
	wait := drainAsync(out, 500*time.Millisecond)
	results := wait()

	opened := filterByEventType(results, "opened")
	if len(opened) != 1 {
		t.Fatalf("expected 1 opened, got %d: %v", len(opened), eventTypes(results))
	}
	if opened[0].Status != StatusOpen {
		t.Errorf("status = %s, want OPEN", opened[0].Status)
	}
	if opened[0].EventType != "opened" {
		t.Errorf("eventType = %s, want opened", opened[0].EventType)
	}
}

func TestLifecycle_DuplicateWithinWindow_Suppressed(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   5 * time.Minute,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	in := make(chan Incident, 2)
	in <- makeIncident("inc-1")
	in <- makeIncident("inc-1")
	close(in)

	out := lm.Run(context.Background(), in)
	results := drainAsync(out, 500*time.Millisecond)()

	opened := filterByEventType(results, "opened")
	updated := filterByEventType(results, "updated")
	if len(opened) != 1 {
		t.Errorf("expected 1 opened, got %d", len(opened))
	}
	if len(updated) != 0 {
		t.Errorf("expected 0 updated (suppressed), got %d", len(updated))
	}
}

func TestLifecycle_DuplicateAfterWindow_EmitsUpdated(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   100 * time.Millisecond,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	var mu sync.Mutex
	fakeNow := time.Now()
	lm.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return fakeNow
	}

	in := make(chan Incident, 2)
	out := lm.Run(context.Background(), in)
	wait := drainAsync(out, 500*time.Millisecond)

	in <- makeIncident("inc-1")
	time.Sleep(30 * time.Millisecond) // let goroutine process

	mu.Lock()
	fakeNow = fakeNow.Add(200 * time.Millisecond)
	mu.Unlock()

	in <- makeIncident("inc-1")
	close(in)

	results := wait()
	opened := filterByEventType(results, "opened")
	updated := filterByEventType(results, "updated")
	if len(opened) != 1 {
		t.Errorf("expected 1 opened, got %d", len(opened))
	}
	if len(updated) != 1 {
		t.Errorf("expected 1 updated, got %d", len(updated))
	}
	if len(updated) > 0 && updated[0].Status != StatusOngoing {
		t.Errorf("updated status = %s, want ONGOING", updated[0].Status)
	}
}

func TestLifecycle_MultipleUpdates_Throttled(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   100 * time.Millisecond,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	var mu sync.Mutex
	fakeNow := time.Now()
	lm.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return fakeNow
	}

	in := make(chan Incident, 5)
	out := lm.Run(context.Background(), in)
	wait := drainAsync(out, 500*time.Millisecond)

	advance := func(d time.Duration) {
		mu.Lock()
		fakeNow = fakeNow.Add(d)
		mu.Unlock()
	}

	// t=0: opened
	in <- makeIncident("inc-1")
	time.Sleep(20 * time.Millisecond)

	// t=60ms: within dedup (100ms) → suppressed
	advance(60 * time.Millisecond)
	in <- makeIncident("inc-1")
	time.Sleep(20 * time.Millisecond)

	// t=160ms: past dedup (100ms since last notified at t=0) → updated
	advance(100 * time.Millisecond)
	in <- makeIncident("inc-1")
	time.Sleep(20 * time.Millisecond)

	// t=220ms: within dedup (60ms since t=160ms) → suppressed
	advance(60 * time.Millisecond)
	in <- makeIncident("inc-1")
	time.Sleep(20 * time.Millisecond)

	// t=320ms: past dedup (160ms since last notified at t=160ms) → updated
	advance(100 * time.Millisecond)
	in <- makeIncident("inc-1")
	close(in)

	results := wait()
	opened := filterByEventType(results, "opened")
	updated := filterByEventType(results, "updated")
	if len(opened) != 1 {
		t.Errorf("expected 1 opened, got %d", len(opened))
	}
	if len(updated) != 2 {
		t.Errorf("expected 2 updated, got %d: %v", len(updated), eventTypes(results))
	}
}

func TestLifecycle_DifferentIDs_Independent(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   5 * time.Minute,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	in := make(chan Incident, 2)
	in <- makeIncident("inc-1")
	in <- makeIncident("inc-2")
	close(in)

	out := lm.Run(context.Background(), in)
	results := drainAsync(out, 500*time.Millisecond)()

	opened := filterByEventType(results, "opened")
	if len(opened) != 2 {
		t.Errorf("expected 2 opened, got %d", len(opened))
	}
}

func TestLifecycle_UpdatedIncident_HasLatestData(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   100 * time.Millisecond,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	var mu sync.Mutex
	fakeNow := time.Now()
	lm.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return fakeNow
	}

	in := make(chan Incident, 2)
	out := lm.Run(context.Background(), in)
	wait := drainAsync(out, 500*time.Millisecond)

	inc1 := makeIncident("inc-1")
	inc1.Diagnosis = "original diagnosis"
	in <- inc1
	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	fakeNow = fakeNow.Add(200 * time.Millisecond)
	mu.Unlock()

	inc2 := makeIncident("inc-1")
	inc2.Diagnosis = "updated diagnosis"
	in <- inc2
	close(in)

	results := wait()
	updated := filterByEventType(results, "updated")
	if len(updated) != 1 {
		t.Fatalf("expected 1 updated, got %d", len(updated))
	}
	if updated[0].Diagnosis != "updated diagnosis" {
		t.Errorf("diagnosis = %q, want %q", updated[0].Diagnosis, "updated diagnosis")
	}
}

// --- Auto-Resolve Tests ---

func TestLifecycle_AutoResolve_AfterTimeout(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Hour,
		ResolveAfter:  100 * time.Millisecond,
		CheckInterval: 50 * time.Millisecond,
	})

	in := make(chan Incident, 1)
	in <- makeIncident("inc-1")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out := lm.Run(ctx, in)
	results := drainAsync(out, 500*time.Millisecond)()

	opened := filterByEventType(results, "opened")
	resolved := filterByEventType(results, "resolved")
	if len(opened) != 1 {
		t.Errorf("expected 1 opened, got %d", len(opened))
	}
	if len(resolved) < 1 {
		t.Errorf("expected at least 1 resolved, got %d: %v", len(resolved), eventTypes(results))
	}
}

func TestLifecycle_AutoResolve_ResetByNewEvent(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Millisecond,
		ResolveAfter:  150 * time.Millisecond,
		CheckInterval: 50 * time.Millisecond,
	})

	in := make(chan Incident)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out := lm.Run(ctx, in)
	wait := drainAsync(out, 500*time.Millisecond)

	in <- makeIncident("inc-1")
	time.Sleep(80 * time.Millisecond)
	in <- makeIncident("inc-1")
	time.Sleep(80 * time.Millisecond)
	close(in)

	results := wait()
	resolved := filterByEventType(results, "resolved")
	if len(resolved) != 1 {
		t.Errorf("expected exactly 1 resolved, got %d: %v", len(resolved), eventTypes(results))
	}
}

func TestLifecycle_Resolved_IncidentHasDuration(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Hour,
		ResolveAfter:  100 * time.Millisecond,
		CheckInterval: 50 * time.Millisecond,
	})

	in := make(chan Incident, 1)
	in <- makeIncident("inc-1")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out := lm.Run(ctx, in)
	results := drainAsync(out, 500*time.Millisecond)()

	resolved := filterByEventType(results, "resolved")
	if len(resolved) < 1 {
		t.Fatalf("expected at least 1 resolved, got %d", len(resolved))
	}
	if resolved[0].Duration <= 0 {
		t.Errorf("expected positive duration, got %v", resolved[0].Duration)
	}
}

func TestLifecycle_Resolved_RemovedFromTracking(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Hour,
		ResolveAfter:  80 * time.Millisecond,
		CheckInterval: 40 * time.Millisecond,
	})

	in := make(chan Incident)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out := lm.Run(ctx, in)
	wait := drainAsync(out, 500*time.Millisecond)

	in <- makeIncident("inc-1")
	time.Sleep(200 * time.Millisecond) // wait for auto-resolve
	in <- makeIncident("inc-1")        // re-open
	close(in)

	results := wait()
	opened := filterByEventType(results, "opened")
	resolved := filterByEventType(results, "resolved")
	if len(opened) != 2 {
		t.Errorf("expected 2 opened (re-opened after resolve), got %d: %v", len(opened), eventTypes(results))
	}
	if len(resolved) < 1 {
		t.Errorf("expected at least 1 resolved, got %d", len(resolved))
	}
}

// --- Shutdown Tests ---

func TestLifecycle_ContextCancel_ResolvesAll(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Hour,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	in := make(chan Incident, 3)
	in <- makeIncident("inc-1")
	in <- makeIncident("inc-2")
	in <- makeIncident("inc-3")

	ctx, cancel := context.WithCancel(context.Background())
	out := lm.Run(ctx, in)
	wait := drainAsync(out, 500*time.Millisecond)

	time.Sleep(100 * time.Millisecond) // let goroutine process buffered items
	cancel()

	results := wait()
	opened := filterByEventType(results, "opened")
	resolved := filterByEventType(results, "resolved")
	if len(opened) != 3 {
		t.Errorf("expected 3 opened, got %d: %v", len(opened), eventTypes(results))
	}
	if len(resolved) != 3 {
		t.Errorf("expected 3 resolved, got %d: %v", len(resolved), eventTypes(results))
	}
}

func TestLifecycle_InputCloses_ResolvesAll(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Hour,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	in := make(chan Incident, 2)
	in <- makeIncident("inc-1")
	in <- makeIncident("inc-2")
	close(in)

	out := lm.Run(context.Background(), in)
	results := drainAsync(out, 500*time.Millisecond)()

	opened := filterByEventType(results, "opened")
	resolved := filterByEventType(results, "resolved")
	if len(opened) != 2 {
		t.Errorf("expected 2 opened, got %d", len(opened))
	}
	if len(resolved) != 2 {
		t.Errorf("expected 2 resolved, got %d: %v", len(resolved), eventTypes(results))
	}
}

func TestLifecycle_EmptyInput_ClosesOutput(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Hour,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	in := make(chan Incident)
	close(in)
	out := lm.Run(context.Background(), in)
	results := drainAsync(out, 500*time.Millisecond)()

	if len(results) != 0 {
		t.Errorf("expected 0 notifications, got %d", len(results))
	}
}

// --- Concurrency Tests ---

func TestLifecycle_ConcurrentResolveAndEvent(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Millisecond,
		ResolveAfter:  50 * time.Millisecond,
		CheckInterval: 20 * time.Millisecond,
	})

	in := make(chan Incident)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := lm.Run(ctx, in)
	wait := drainAsync(out, 500*time.Millisecond)

	go func() {
		for i := 0; i < 10; i++ {
			select {
			case in <- makeIncident("race-inc"):
			case <-ctx.Done():
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
		close(in)
	}()

	results := wait()
	opened := filterByEventType(results, "opened")
	if len(opened) < 1 {
		t.Errorf("expected at least 1 opened, got %d", len(opened))
	}
}

func TestLifecycle_RaceDetector(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Millisecond,
		ResolveAfter:  20 * time.Millisecond,
		CheckInterval: 10 * time.Millisecond,
	})

	in := make(chan Incident)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := lm.Run(ctx, in)
	wait := drainAsync(out, 500*time.Millisecond)

	go func() {
		for i := 0; i < 100; i++ {
			id := "id-" + string(rune('a'+i%20))
			select {
			case in <- makeIncident(id):
			case <-ctx.Done():
				return
			}
		}
		close(in)
	}()

	wait() // just verify no race
}

// --- Edge Cases ---

func TestLifecycle_ZeroDedupWindow(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   0,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	var mu sync.Mutex
	fakeNow := time.Now()
	lm.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return fakeNow
	}

	advance := func(d time.Duration) {
		mu.Lock()
		fakeNow = fakeNow.Add(d)
		mu.Unlock()
	}

	in := make(chan Incident, 3)
	out := lm.Run(context.Background(), in)
	wait := drainAsync(out, 500*time.Millisecond)

	in <- makeIncident("inc-1")
	time.Sleep(20 * time.Millisecond)
	advance(time.Nanosecond)

	in <- makeIncident("inc-1")
	time.Sleep(20 * time.Millisecond)
	advance(time.Nanosecond)

	in <- makeIncident("inc-1")
	close(in)

	results := wait()
	updated := filterByEventType(results, "updated")
	if len(updated) != 2 {
		t.Errorf("expected 2 updated with DedupWindow=0, got %d: %v", len(updated), eventTypes(results))
	}
}

func TestLifecycle_VeryShortResolve(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   time.Millisecond,
		ResolveAfter:  time.Millisecond,
		CheckInterval: time.Millisecond,
	})

	in := make(chan Incident, 1)
	in <- makeIncident("inc-1")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := lm.Run(ctx, in)
	wait := drainAsync(out, 500*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	close(in)

	results := wait()
	resolved := filterByEventType(results, "resolved")
	if len(resolved) < 1 {
		t.Errorf("expected at least 1 resolved with short timeout, got %d: %v", len(resolved), eventTypes(results))
	}
}

// --- Integration Test ---

func TestPipeline_LifecycleToDispatcher(t *testing.T) {
	lm := NewLifecycleManager(LifecycleConfig{
		DedupWindow:   100 * time.Millisecond,
		ResolveAfter:  time.Hour,
		CheckInterval: time.Hour,
	})

	var mu sync.Mutex
	fakeNow := time.Now()
	lm.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return fakeNow
	}

	mock := &mockNotifier{}
	dispatcher := NewDispatcher(mock)

	in := make(chan Incident, 3)
	out := lm.Run(context.Background(), in)

	in <- makeIncident("inc-1")
	time.Sleep(20 * time.Millisecond)
	in <- makeIncident("inc-1") // duplicate, suppressed
	time.Sleep(20 * time.Millisecond)
	in <- makeIncident("inc-2")
	close(in)

	for inc := range out {
		if err := dispatcher.Dispatch(context.Background(), inc); err != nil {
			t.Errorf("dispatch failed: %v", err)
		}
	}

	received := mock.getIncidents()
	openedCount := 0
	resolvedCount := 0
	for _, r := range received {
		switch r.EventType {
		case "opened":
			openedCount++
		case "resolved":
			resolvedCount++
		}
	}
	if openedCount != 2 {
		t.Errorf("expected 2 opened dispatched, got %d", openedCount)
	}
	if resolvedCount != 2 {
		t.Errorf("expected 2 resolved dispatched, got %d", resolvedCount)
	}
}
