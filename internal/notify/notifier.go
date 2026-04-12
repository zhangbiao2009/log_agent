package notify

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// PatternSummary is a per-pattern rollup within an alert.
type PatternSummary struct {
	Template    string
	Count       int
	Level       string
	SampleLines []string // up to 3 representative lines
}

// Alert is the data sent to notification channels.
type Alert struct {
	Service     string
	Level       string           // highest severity seen (FATAL > ERROR > WARN)
	Count       int              // number of error lines in the window
	Window      time.Duration    // aggregation window
	SampleLines []string         // up to 5 example log lines (Phase 1 fallback)
	Patterns    []PatternSummary // per-pattern breakdown (empty if pattern engine disabled)
	Timestamp   time.Time        // window end time
}

// Notifier is the interface all notification channels implement.
type Notifier interface {
	Send(ctx context.Context, alert Alert) error
	Name() string
}

// Dispatcher fans out alerts to all registered notifiers.
type Dispatcher struct {
	notifiers []Notifier
	timeout   time.Duration
}

// NewDispatcher creates a Dispatcher with the given notifiers.
func NewDispatcher(notifiers ...Notifier) *Dispatcher {
	return &Dispatcher{
		notifiers: notifiers,
		timeout:   10 * time.Second,
	}
}

// Dispatch sends the alert to all notifiers concurrently.
// Logs errors from individual notifiers but does not fail the pipeline.
// Returns an error if any notifier failed.
func (d *Dispatcher) Dispatch(ctx context.Context, alert Alert) error {
	if len(d.notifiers) == 0 {
		return nil
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []string
	)

	for _, n := range d.notifiers {
		wg.Add(1)
		go func(n Notifier) {
			defer wg.Done()
			sendCtx, cancel := context.WithTimeout(ctx, d.timeout)
			defer cancel()

			if err := n.Send(sendCtx, alert); err != nil {
				slog.Error("notifier failed", "notifier", n.Name(), "err", err)
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", n.Name(), err))
				mu.Unlock()
			}
		}(n)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("dispatch errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
