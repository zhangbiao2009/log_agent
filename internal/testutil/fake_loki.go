package testutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
)

type LokiLogEntry struct {
	Service   string
	Timestamp string
	Line      string
}

type FakeLoki struct {
	Server       *httptest.Server
	mu           sync.Mutex
	responses    [][]LokiLogEntry
	responseIdx  int
	requestCount atomic.Int64
	lastStart    string
}

func NewFakeLoki() *FakeLoki {
	f := &FakeLoki{}
	f.Server = httptest.NewServer(http.HandlerFunc(f.handler))
	return f
}

func (f *FakeLoki) AddResponse(entries []LokiLogEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses = append(f.responses, entries)
}

func (f *FakeLoki) URL() string    { return f.Server.URL }
func (f *FakeLoki) Close()         { f.Server.Close() }
func (f *FakeLoki) RequestCount() int64 { return f.requestCount.Load() }

func (f *FakeLoki) LastStartParam() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastStart
}

func (f *FakeLoki) handler(w http.ResponseWriter, r *http.Request) {
	f.requestCount.Add(1)
	f.mu.Lock()
	f.lastStart = r.URL.Query().Get("start")
	var entries []LokiLogEntry
	if f.responseIdx < len(f.responses) {
		entries = f.responses[f.responseIdx]
		f.responseIdx++
	}
	f.mu.Unlock()
	resp := buildLokiResponse(entries)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func buildLokiResponse(entries []LokiLogEntry) map[string]interface{} {
	byService := make(map[string][][]string)
	for _, e := range entries {
		byService[e.Service] = append(byService[e.Service], []string{e.Timestamp, e.Line})
	}
	var result []map[string]interface{}
	for svc, vals := range byService {
		result = append(result, map[string]interface{}{
			"stream": map[string]string{"service": svc, "namespace": "prod"},
			"values": vals,
		})
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "streams",
			"result":     result,
		},
	}
}
