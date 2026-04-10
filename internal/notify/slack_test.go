package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSlackNotifier_Send(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type = %s, want application/json", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	alert := Alert{
		Service: "myapp",
		Level:   "ERROR",
		Count:   42,
		Window:  5 * time.Minute,
		SampleLines: []string{
			"connection refused",
			"disk <full> & slow",
		},
		Timestamp: time.Now(),
	}

	err := sn.Send(context.Background(), alert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify JSON is valid
	var msg map[string]interface{}
	if err := json.Unmarshal(receivedBody, &msg); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	blocks, ok := msg["blocks"].([]interface{})
	if !ok {
		t.Fatal("expected blocks array")
	}
	if len(blocks) < 2 {
		t.Fatalf("expected at least 2 blocks, got %d", len(blocks))
	}

	// Verify HTML escaping in sample block
	sampleBlock := blocks[1].(map[string]interface{})
	text := sampleBlock["text"].(map[string]interface{})["text"].(string)
	if !contains(text, "&lt;full&gt;") {
		t.Errorf("expected HTML-escaped angle brackets in %q", text)
	}
	if !contains(text, "&amp;") {
		t.Errorf("expected HTML-escaped ampersand in %q", text)
	}
}

func TestSlackNotifier_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("webhook error"))
	}))
	defer srv.Close()

	sn := NewSlackNotifier(srv.URL)
	err := sn.Send(context.Background(), Alert{Service: "svc", Level: "ERROR", Count: 1, Window: time.Minute})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestSlackNotifier_Name(t *testing.T) {
	sn := NewSlackNotifier("http://example.com")
	if sn.Name() != "slack" {
		t.Errorf("name = %s, want slack", sn.Name())
	}
}

func TestLevelEmoji(t *testing.T) {
	tests := []struct {
		level string
		emoji string
	}{
		{"FATAL", "\U0001F480"},
		{"ERROR", "\U0001F534"},
		{"WARN", "\U0001F7E1"},
		{"UNKNOWN", "\u2139\uFE0F"},
	}
	for _, tc := range tests {
		got := levelEmoji(tc.level)
		if got != tc.emoji {
			t.Errorf("levelEmoji(%q) = %q, want %q", tc.level, got, tc.emoji)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsImpl(s, substr))
}

func containsImpl(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
