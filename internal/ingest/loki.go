package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type LokiConfig struct {
	URL          string        `yaml:"url"`
	Query        string        `yaml:"query"`
	PollInterval time.Duration `yaml:"poll_interval"`
}

type LokiSource struct {
	config LokiConfig
	client *http.Client
}

func NewLokiSource(cfg LokiConfig) *LokiSource {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Second
	}
	return &LokiSource{
		config: cfg,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *LokiSource) Stream(ctx context.Context) (<-chan LogLine, error) {
	out := make(chan LogLine, 10000)
	go s.poll(ctx, out)
	return out, nil
}

func (s *LokiSource) poll(ctx context.Context, out chan<- LogLine) {
	defer close(out)
	start := time.Now()
	consecutiveFailures := 0

	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	// Pipeline parallelism: fetch+parse returns lines immediately, then a
	// background goroutine sends them to the channel while the next fetch
	// can proceed. A sendSem channel limits in-flight send goroutines to 2
	// to prevent goroutine pile-up if the consumer is slow.
	var sendWg sync.WaitGroup
	sendSem := make(chan struct{}, 2)
	defer sendWg.Wait()

	s.doPoll(ctx, out, &start, &consecutiveFailures, &sendWg, sendSem)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.doPoll(ctx, out, &start, &consecutiveFailures, &sendWg, sendSem)
		}
	}
}

func (s *LokiSource) doPoll(ctx context.Context, out chan<- LogLine, start *time.Time, consecutiveFailures *int, sendWg *sync.WaitGroup, sendSem chan struct{}) {
	end := time.Now()
	lines, maxTS, err := s.fetchLogs(ctx, *start, end)
	if err != nil {
		*consecutiveFailures++
		if *consecutiveFailures >= 3 {
			slog.Error("loki poll failed (consecutive)", "failures", *consecutiveFailures, "err", err)
		} else {
			slog.Warn("loki poll failed", "err", err)
		}
		return
	}
	*consecutiveFailures = 0

	// Advance start immediately so the next fetch can overlap with sending.
	if !maxTS.IsZero() {
		*start = maxTS.Add(1 * time.Nanosecond)
	} else {
		*start = end
	}

	// Send lines to channel in background, allowing the next poll to start.
	// Acquire semaphore to limit concurrent senders (blocks if 2 already in-flight).
	if len(lines) > 0 {
		select {
		case sendSem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		sendWg.Add(1)
		go func(lines []LogLine) {
			defer sendWg.Done()
			defer func() { <-sendSem }()
			for _, line := range lines {
				select {
				case out <- line:
				case <-ctx.Done():
					return
				}
			}
		}(lines)
	}
}

type lokiResponse struct {
	Status string   `json:"status"`
	Data   lokiData `json:"data"`
}
type lokiData struct {
	ResultType string       `json:"resultType"`
	Result     []lokiStream `json:"result"`
}
type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func (s *LokiSource) fetchLogs(ctx context.Context, start, end time.Time) ([]LogLine, time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.config.URL+"/loki/api/v1/query_range", nil)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("build request: %w", err)
	}
	q := req.URL.Query()
	q.Set("query", s.config.Query)
	q.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	q.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	q.Set("direction", "forward")
	q.Set("limit", "5000")
	req.URL.RawQuery = q.Encode()

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, time.Time{}, fmt.Errorf("loki returned %d: %s", resp.StatusCode, string(body))
	}
	return parseLokiResponse(resp.Body)
}

func parseLokiResponse(r io.Reader) ([]LogLine, time.Time, error) {
	// PERF: This function is the single largest allocation site (~5k allocs
	// per 1000-line response). Three improvements to consider:
	//   1. Pre-allocate the lines slice: estimate capacity from Content-Length
	//      or response size to avoid repeated slice growth.
	//   2. Pool []LogLine slices with sync.Pool to reuse across poll cycles,
	//      since each poll produces a similarly-sized batch.
	//   3. Use a streaming JSON tokenizer (json.Decoder.Token()) to parse
	//      values directly instead of materializing the full response struct,
	//      which would cut the string copy allocations for Raw fields.
	var resp lokiResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return nil, time.Time{}, fmt.Errorf("decode response: %w", err)
	}
	var lines []LogLine
	var maxTS time.Time
	for _, stream := range resp.Data.Result {
		service := stream.Stream["service"]
		if service == "" {
			for _, key := range []string{"app", "container", "job"} {
				if v := stream.Stream[key]; v != "" {
					service = v
					break
				}
			}
			if service == "" {
				service = "unknown"
			}
		}
		for _, val := range stream.Values {
			if len(val) < 2 {
				continue
			}
			ts, err := strconv.ParseInt(val[0], 10, 64)
			if err != nil {
				continue
			}
			t := time.Unix(0, ts)
			if t.After(maxTS) {
				maxTS = t
			}
			lines = append(lines, LogLine{
				Service:   service,
				Timestamp: t,
				Raw:       val[1],
			})
		}
	}
	return lines, maxTS, nil
}
