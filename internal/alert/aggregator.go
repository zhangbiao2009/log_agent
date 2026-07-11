package alert

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/core"
	"github.com/zhangbiao2009/log_agent/internal/ingest"
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

type bucketKey struct {
	service   string
	patternID string
}

type bucket struct {
	count       int
	highestRank int
	level       string
	template    string
	samples     []string
}

func (b *bucket) add(line ingest.LogLine) {
	b.count++
	rank := severityRank(line.Level)
	if rank > b.highestRank {
		b.highestRank = rank
		b.level = line.Level
	}
	if b.template == "" && line.PatternTemplate != "" {
		b.template = line.PatternTemplate
	}
	if len(b.samples) < 5 {
		b.samples = append(b.samples, line.Raw)
	}
}

type Aggregator struct {
	Window   time.Duration
	MinCount int
	Clock    core.Clock
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
		Clock:    core.RealClock(),
	}
}

func (a *Aggregator) Run(ctx context.Context, in <-chan ingest.LogLine) <-chan core.Alert {
	out := make(chan core.Alert, 100)
	go func() {
		defer close(out)
		buckets := make(map[bucketKey]*bucket)
		timer := a.Clock.After(a.Window)
		for {
			select {
			case <-ctx.Done():
				a.flush(buckets, out)
				return
			case <-timer:
				a.flush(buckets, out)
				buckets = make(map[bucketKey]*bucket)
				timer = a.Clock.After(a.Window)
			case line, ok := <-in:
				if !ok {
					a.flush(buckets, out)
					return
				}
				key := bucketKey{service: line.Service, patternID: line.PatternID}
				b, exists := buckets[key]
				if !exists {
					b = &bucket{}
					buckets[key] = b
				}
				b.add(line)
			}
		}
	}()
	return out
}

func (a *Aggregator) flush(buckets map[bucketKey]*bucket, out chan<- core.Alert) {
	now := a.Clock.Now()

	type serviceInfo struct {
		totalCount  int
		highestRank int
		level       string
		samples     []string
		patterns    []core.PatternSummary
	}
	serviceMap := make(map[string]*serviceInfo)

	for key, b := range buckets {
		si, exists := serviceMap[key.service]
		if !exists {
			si = &serviceInfo{}
			serviceMap[key.service] = si
		}
		si.totalCount += b.count
		if b.highestRank > si.highestRank {
			si.highestRank = b.highestRank
			si.level = b.level
		}
		if len(si.samples) < 5 {
			si.samples = append(si.samples, b.samples...)
			if len(si.samples) > 5 {
				si.samples = si.samples[:5]
			}
		}
		if key.patternID != "" {
			psamples := b.samples
			if len(psamples) > 3 {
				psamples = psamples[:3]
			}
			si.patterns = append(si.patterns, core.PatternSummary{
				Template:    b.template,
				Count:       b.count,
				Level:       b.level,
				SampleLines: psamples,
			})
		}
	}

	for service, si := range serviceMap {
		if si.totalCount < a.MinCount {
			slog.Debug("skipping alert below threshold", "service", service, "count", si.totalCount, "min", a.MinCount)
			continue
		}
		sort.Slice(si.patterns, func(i, j int) bool {
			return si.patterns[i].Count > si.patterns[j].Count
		})
		out <- core.Alert{
			Service:     service,
			Level:       si.level,
			Count:       si.totalCount,
			Window:      a.Window,
			SampleLines: si.samples,
			Patterns:    si.patterns,
			Timestamp:   now,
		}
	}
}
