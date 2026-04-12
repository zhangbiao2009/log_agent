package correlator

import (
	"context"

	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/notify"
)

// WrapAlerts is the bypass path when the correlator is disabled.
// Each alert becomes a single-alert Incident with no correlation metadata.
func WrapAlerts(ctx context.Context, in <-chan notify.Alert) <-chan notify.Incident {
	out := make(chan notify.Incident, cap(in))
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
				inc := notify.Incident{
					Alerts:   []notify.Alert{alert},
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
