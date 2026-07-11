package correlator

import (
	"context"

	"github.com/zhangbiao2009/log_agent/internal/core"
)

// WrapAlerts is the bypass path when the correlator is disabled.
// Each alert becomes a single-alert Incident with no correlation metadata.
func WrapAlerts(ctx context.Context, in <-chan core.Alert) <-chan core.Incident {
	out := make(chan core.Incident, cap(in))
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case alert, ok := <-in:
				if !ok {
					return
				}
				inc := core.Incident{
					Alerts:   []core.Alert{alert},
					Services: []string{alert.Service},
					OpenedAt: alert.Timestamp,
					Window:   alert.Window,
				}
				select {
				case out <- inc:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
