package notify

import (
	"context"
	"log/slog"
	"time"

	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/ingest"
)

func severityRank(level string) int {
	switch level {
	case "FATAL":
		return 3
	case "ERROR":
		return 2
	case "WARN":
		return 1
	default:
		return 0
	}
}

type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                        { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type bucket struct {
	count       int
	highestRank int
	level       string
	samples     []string
}

func (b *bucket) add(line ingest.LogLine) {
	b.count++
	rank := severityRank(line.Level)
	if rank > b.highestRank {
		b.highestRank = rank
		b.level = line.Level
	}
	if len(b.samples) < 5 {
		b.samples = append(b.samples, line.Raw)
	}
}

type Aggregator struct {
	Window   time.Duration
	MinCount int
	Clock    Clock
}

func NewAggregator(window time.Duration, minCount int) *Aggregator {
	if window == 0 {
		window = 1 * time.Minute
	}
	if minCount == 0 {
		minCount = 1
	}
	return &Aggregator{
		Window:   window,
		MinCount: minCount,
		Clock:    realClock{},
	}
}

func (a *Aggregator) Run(ctx context.Context, in <-chan ingest.LogLine) <-chan Alert {
	out := make(chan Alert, 100)
	go func() {
		defer close(out)
		buckets := make(map[string]*bucket)
		timer := a.Clock.After(a.Window)
		for {
			select {
			case <-ctx.Done():
				a.flush(buckets, out)
				return
			case <-timer:
				a.flush(buckets, out)
				buckets = make(map[string]*bucket)
				timer = a.Clock.After(a.Window)
			case line, ok := <-in:
				if !ok {
					a.flush(buckets, out)
					return
				}
				b, exists := buckets[line.Service]
				if !exists {
					b = &bucket{}
					buckets[line.Service] = b
				}
				b.add(line)
			}
		}
	}()
	return out
}

func (a *Aggregator) flush(buckets map[string]*bucket, out chan<- Alert) {
	now := a.Clock.Now()
	for service, b := range buckets {
		if b.count < a.MinCount {
			slog.Debug("skipping alert below threshold", "service", service, "count", b.count, "min", a.MinCount)
			continue
		}
		out <- Alert{
			Service:     service,
			Level:       b.level,
			Count:       b.count,
			Window:      a.Window,
			SampleLines: b.samples,
			Timestamp:   now,
		}
	}
}
