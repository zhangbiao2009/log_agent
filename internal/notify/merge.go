package notify

import (
	"context"
	"sync"
)

// MergeAlerts fans in multiple per-service Alert channels into a single
// channel. It is the synchronization point between the per-service zone
// (Filter → Pattern → Aggregator → Anomaly, one pipeline per service) and
// the shared zone (Correlator → Diagnoser → Lifecycle).
//
// The returned channel is closed once all input channels are closed or ctx
// is cancelled. Ordering across services is not guaranteed; the Correlator
// groups alerts by time window, so interleaving is expected and fine.
func MergeAlerts(ctx context.Context, ins ...<-chan Alert) <-chan Alert {
	out := make(chan Alert, 100)
	var wg sync.WaitGroup
	wg.Add(len(ins))
	for _, in := range ins {
		go func(in <-chan Alert) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case alert, ok := <-in:
					if !ok {
						return
					}
					select {
					case out <- alert:
					case <-ctx.Done():
						return
					}
				}
			}
		}(in)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
