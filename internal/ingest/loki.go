package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type LokiConfig struct {
	URL               string        `yaml:"url"`
	Query             string        `yaml:"query"`
	PollInterval      time.Duration `yaml:"poll_interval"`
	TenantID          string        `yaml:"tenant_id"`
	ServiceLabel      string        `yaml:"service_label"`
	BasicAuthUser     string        `yaml:"basic_auth_user"`
	BasicAuthPassword string        `yaml:"basic_auth_password"`

	// Service, when non-empty, forces the service name for every line this
	// source emits, bypassing label extraction. Used in the per-service
	// pipeline model where each source targets a single service via Query.
	Service string `yaml:"service"`
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

	s.doPoll(ctx, out, &start, &consecutiveFailures)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.doPoll(ctx, out, &start, &consecutiveFailures)
		}
	}
}

func (s *LokiSource) doPoll(ctx context.Context, out chan<- LogLine, start *time.Time, consecutiveFailures *int) {
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

	slog.Info("loki poll complete", "lines", len(lines), "start", start.Format(time.RFC3339), "end", end.Format(time.RFC3339))

	// Advance start so the next fetch picks up where this one ended.
	if !maxTS.IsZero() {
		*start = maxTS.Add(1 * time.Nanosecond)
	} else {
		*start = end
	}

	// Send lines to the channel. Blocks on a slow consumer, which naturally
	// throttles polling (backpressure).
	for _, line := range lines {
		select {
		case out <- line:
		case <-ctx.Done():
			return
		}
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
	if s.config.TenantID != "" {
		req.Header.Set("X-Scope-OrgID", s.config.TenantID)
	}
	if s.config.BasicAuthUser != "" {
		req.SetBasicAuth(s.config.BasicAuthUser, s.config.BasicAuthPassword)
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
	return parseLokiResponse(resp.Body, s.config.ServiceLabel, s.config.Service)
}

// extractService picks the service name from stream labels.
// If serviceLabel is set, it uses that key directly.
// Otherwise it tries a fallback chain: service → app → container → job → unknown.
func extractService(labels map[string]string, serviceLabel string) string {
	if serviceLabel != "" {
		if v := labels[serviceLabel]; v != "" {
			return v
		}
		return "unknown"
	}
	for _, key := range []string{"service", "app", "container", "job"} {
		if v := labels[key]; v != "" {
			return v
		}
	}
	return "unknown"
}

func parseLokiResponse(r io.Reader, serviceLabel string, serviceOverride string) ([]LogLine, time.Time, error) {
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
		service := extractService(stream.Stream, serviceLabel)
		if serviceOverride != "" {
			service = serviceOverride
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
