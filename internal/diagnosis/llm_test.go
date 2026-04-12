package diagnosis

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClient_RequestFormat(t *testing.T) {
	var reqBody []byte
	var reqMethod string
	var reqPath string
	var contentType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqMethod = r.Method
		reqPath = r.URL.Path
		contentType = r.Header.Get("Content-Type")
		reqBody, _ = io.ReadAll(r.Body)
		json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{{Message: chatMessage{Role: "assistant", Content: "ok"}}},
		})
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{
		Endpoint:    srv.URL + "/v1/chat/completions",
		Model:       "test-model",
		MaxTokens:   512,
		Temperature: 0,
	}, "sk-test")

	_, err := client.Complete(context.Background(), "test prompt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reqMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", reqMethod)
	}
	if reqPath != "/v1/chat/completions" {
		t.Errorf("path = %q, want /v1/chat/completions", reqPath)
	}
	if contentType != "application/json" {
		t.Errorf("content-type = %q, want application/json", contentType)
	}

	var req chatRequest
	if err := json.Unmarshal(reqBody, &req); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	if req.Model != "test-model" {
		t.Errorf("model = %q, want test-model", req.Model)
	}
	if len(req.Messages) != 1 || req.Messages[0].Role != "user" || req.Messages[0].Content != "test prompt" {
		t.Errorf("messages = %+v, want [{user, test prompt}]", req.Messages)
	}
	if req.MaxTokens != 512 {
		t.Errorf("max_tokens = %d, want 512", req.MaxTokens)
	}
	if req.Temperature != 0 {
		t.Errorf("temperature = %f, want 0", req.Temperature)
	}
}

func TestHTTPClient_AuthorizationHeader(t *testing.T) {
	var authHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{{Message: chatMessage{Role: "assistant", Content: "ok"}}},
		})
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{Endpoint: srv.URL}, "sk-test-123")
	_, err := client.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if authHeader != "Bearer sk-test-123" {
		t.Errorf("Authorization = %q, want 'Bearer sk-test-123'", authHeader)
	}
}

func TestHTTPClient_SuccessfulResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{{Message: chatMessage{Role: "assistant", Content: "diagnosis result"}}},
		})
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{Endpoint: srv.URL}, "key")
	result, err := client.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "diagnosis result" {
		t.Errorf("result = %q, want 'diagnosis result'", result)
	}
}

func TestHTTPClient_EmptyChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(chatResponse{Choices: []chatChoice{}})
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{Endpoint: srv.URL}, "key")
	_, err := client.Complete(context.Background(), "prompt")
	if err == nil {
		t.Fatal("expected error for empty choices")
	}
	if !strings.Contains(err.Error(), "empty choices") {
		t.Errorf("error = %v, want 'empty choices'", err)
	}
}

func TestHTTPClient_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{Endpoint: srv.URL}, "key")
	_, err := client.Complete(context.Background(), "prompt")
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestHTTPClient_RateLimitRetry(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("rate limited"))
			return
		}
		json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{{Message: chatMessage{Role: "assistant", Content: "success"}}},
		})
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{Endpoint: srv.URL}, "key")
	result, err := client.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "success" {
		t.Errorf("result = %q, want 'success'", result)
	}
	if atomic.LoadInt32(&callCount) != 2 {
		t.Errorf("callCount = %d, want 2", callCount)
	}
}

func TestHTTPClient_ServerErrorRetry(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
			return
		}
		json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{{Message: chatMessage{Role: "assistant", Content: "success"}}},
		})
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{Endpoint: srv.URL}, "key")
	result, err := client.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "success" {
		t.Errorf("result = %q, want 'success'", result)
	}
	if atomic.LoadInt32(&callCount) != 2 {
		t.Errorf("callCount = %d, want 2", callCount)
	}
}

func TestHTTPClient_ClientErrorNoRetry(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad request"))
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{Endpoint: srv.URL}, "key")
	_, err := client.Complete(context.Background(), "prompt")
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("callCount = %d, want 1 (no retry on 4xx)", callCount)
	}
}

func TestHTTPClient_RespectsContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delay longer than the client's context timeout.
		time.Sleep(5 * time.Second)
	}))
	defer srv.Close()

	client := NewHTTPClient(DiagnoserConfig{
		Endpoint: srv.URL,
		Timeout:  10 * time.Second,
	}, "key")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.Complete(ctx, "prompt")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
	if elapsed > 2*time.Second {
		t.Errorf("Complete took %v, expected < 2s (context should cancel quickly)", elapsed)
	}
}
