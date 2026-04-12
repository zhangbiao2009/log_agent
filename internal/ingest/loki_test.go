package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newFakeLokiServer(responses []string) (*httptest.Server, *atomic.Int64) {
	var idx atomic.Int64
	var count atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		i := idx.Add(1) - 1
		if int(i) < len(responses) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(responses[i]))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[]}}`))
		}
	}))
	return server, &count
}

func makeLokiResponseJSON(entries []struct {
	Service string
	TS      int64
	Line    string
}) string {
	byService := make(map[string][][]string)
	for _, e := range entries {
		byService[e.Service] = append(byService[e.Service], []string{
			strconv.FormatInt(e.TS, 10),
			e.Line,
		})
	}
	var result []map[string]interface{}
	for svc, vals := range byService {
		result = append(result, map[string]interface{}{
			"stream": map[string]string{"service": svc},
			"values": vals,
		})
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	resp := map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "streams",
			"result":     result,
		},
	}
	b, _ := json.Marshal(resp)
	return string(b)
}

func TestParseLokiResponse_Basic(t *testing.T) {
	ts := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC).UnixNano()
	body := makeLokiResponseJSON([]struct {
		Service string
		TS      int64
		Line    string
	}{
		{"myapp", ts, `{"level":"error","msg":"bad"}`},
		{"myapp", ts + 1, `another line`},
	})
	lines, maxTS, err := parseLokiResponse(strings.NewReader(body), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if lines[0].Service != "myapp" {
		t.Errorf("expected service myapp, got %s", lines[0].Service)
	}
	expectedMaxTS := time.Unix(0, ts+1)
	if !maxTS.Equal(expectedMaxTS) {
		t.Errorf("maxTS = %v, want %v", maxTS, expectedMaxTS)
	}
}

func TestParseLokiResponse_ServiceFallback(t *testing.T) {
	resp := `{"status":"success","data":{"resultType":"streams","result":[
		{"stream":{"app":"frontend","namespace":"prod"},"values":[["1000","line1"]]},
		{"stream":{"container":"worker"},"values":[["2000","line2"]]},
		{"stream":{"job":"batch"},"values":[["3000","line3"]]},
		{"stream":{"namespace":"prod"},"values":[["4000","line4"]]}
	]}}`
	lines, _, err := parseLokiResponse(strings.NewReader(resp), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []string{"frontend", "worker", "batch", "unknown"}
	for i, want := range expected {
		if lines[i].Service != want {
			t.Errorf("line %d: service = %q, want %q", i, lines[i].Service, want)
		}
	}
}

func TestParseLokiResponse_Empty(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"streams","result":[]}}`
	lines, maxTS, err := parseLokiResponse(strings.NewReader(body), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("expected 0 lines, got %d", len(lines))
	}
	if !maxTS.IsZero() {
		t.Errorf("expected zero maxTS, got %v", maxTS)
	}
}

func TestParseLokiResponse_CustomServiceLabel(t *testing.T) {
	resp := `{"status":"success","data":{"resultType":"streams","result":[
		{"stream":{"containerId":"host/k3s.service","service_name":"promtail","job":"promtail"},"values":[["1000","line1"]]},
		{"stream":{"service_name":"myapp","job":"batch"},"values":[["2000","line2"]]},
		{"stream":{"job":"orphan"},"values":[["3000","line3"]]}
	]}}`

	// With custom service label "service_name"
	lines, _, err := parseLokiResponse(strings.NewReader(resp), "service_name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []string{"promtail", "myapp", "unknown"}
	for i, want := range expected {
		if lines[i].Service != want {
			t.Errorf("line %d: service = %q, want %q", i, lines[i].Service, want)
		}
	}

	// With custom label "containerId"
	lines2, _, err := parseLokiResponse(strings.NewReader(resp), "containerId")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lines2[0].Service != "host/k3s.service" {
		t.Errorf("line 0: service = %q, want %q", lines2[0].Service, "host/k3s.service")
	}
}

func TestLokiSource_Stream(t *testing.T) {
	ts := time.Now().UnixNano()
	resp := makeLokiResponseJSON([]struct {
		Service string
		TS      int64
		Line    string
	}{
		{"svc1", ts, "ERROR: something broke"},
	})
	srv, reqCount := newFakeLokiServer([]string{resp})
	defer srv.Close()

	cfg := LokiConfig{
		URL:          srv.URL,
		Query:        `{namespace="prod"}`,
		PollInterval: 50 * time.Millisecond,
	}
	src := NewLokiSource(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	ch, err := src.Stream(ctx)
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	var lines []LogLine
	for line := range ch {
		lines = append(lines, line)
	}

	if len(lines) < 1 {
		t.Fatalf("expected at least 1 line, got %d", len(lines))
	}
	if lines[0].Service != "svc1" {
		t.Errorf("service = %q, want svc1", lines[0].Service)
	}
	if reqCount.Load() < 1 {
		t.Errorf("expected at least 1 request, got %d", reqCount.Load())
	}
}

func TestLokiSource_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	cfg := LokiConfig{
		URL:          srv.URL,
		Query:        `{namespace="prod"}`,
		PollInterval: 50 * time.Millisecond,
	}
	src := NewLokiSource(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch, err := src.Stream(ctx)
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	var count int
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 lines from erroring server, got %d", count)
	}
}
