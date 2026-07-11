package notify

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Notifier is the interface all notification channels implement.
type Notifier interface {
	Send(ctx context.Context, incident Incident) error
	Name() string
}

// NotifierRoute pairs a Notifier with the severities it should handle.
type NotifierRoute struct {
	Notifier   Notifier
	Severities []string // empty = all severities
}

// routedNotifier wraps a notifier with a severity filter.
type routedNotifier struct {
	notifier   Notifier
	severities map[string]bool // nil or empty = accept all
}

func (rn routedNotifier) matches(severity string) bool {
	if len(rn.severities) == 0 {
		return true
	}
	return rn.severities[severity]
}

// Dispatcher fans out alerts to registered notifiers, filtered by severity.
type Dispatcher struct {
	notifiers []routedNotifier
	timeout   time.Duration
}

// NewDispatcher creates a Dispatcher with the given notifiers (no severity filter).
func NewDispatcher(notifiers ...Notifier) *Dispatcher {
	routed := make([]routedNotifier, len(notifiers))
	for i, n := range notifiers {
		routed[i] = routedNotifier{notifier: n}
	}
	return &Dispatcher{
		notifiers: routed,
		timeout:   10 * time.Second,
	}
}

// NewRoutedDispatcher creates a Dispatcher with per-notifier severity routing.
func NewRoutedDispatcher(routes []NotifierRoute) *Dispatcher {
	routed := make([]routedNotifier, len(routes))
	for i, r := range routes {
		sevMap := make(map[string]bool, len(r.Severities))
		for _, s := range r.Severities {
			sevMap[s] = true
		}
		routed[i] = routedNotifier{notifier: r.Notifier, severities: sevMap}
	}
	return &Dispatcher{
		notifiers: routed,
		timeout:   10 * time.Second,
	}
}

// Dispatch sends the incident to all notifiers whose severity filter matches.
// Logs errors from individual notifiers but does not fail the pipeline.
// Returns an error if any notifier failed.
func (d *Dispatcher) Dispatch(ctx context.Context, incident Incident) error {
	if len(d.notifiers) == 0 {
		return nil
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []string
	)

	for _, rn := range d.notifiers {
		if !rn.matches(incident.Severity) {
			continue
		}
		wg.Add(1)
		go func(rn routedNotifier) {
			defer wg.Done()
			sendCtx, cancel := context.WithTimeout(ctx, d.timeout)
			defer cancel()

			if err := rn.notifier.Send(sendCtx, incident); err != nil {
				slog.Error("notifier failed", "notifier", rn.notifier.Name(), "err", err)
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", rn.notifier.Name(), err))
				mu.Unlock()
			}
		}(rn)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("dispatch errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
