package diagnosis

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// LLMClient abstracts the LLM provider for testing and swapping implementations.
type LLMClient interface {
	Complete(ctx context.Context, prompt string) (string, error)
}

// HTTPClient implements LLMClient using the OpenAI-compatible chat completions API.
type HTTPClient struct {
	config DiagnoserConfig
	apiKey string
	client *http.Client
}

// NewHTTPClient creates an HTTP-based LLM client.
func NewHTTPClient(cfg DiagnoserConfig, apiKey string) *HTTPClient {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &HTTPClient{
		config: cfg,
		apiKey: apiKey,
		client: &http.Client{Timeout: timeout},
	}
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
	Temperature float64       `json:"temperature"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
}

type chatChoice struct {
	Message chatMessage `json:"message"`
}

func (c *HTTPClient) Complete(ctx context.Context, prompt string) (string, error) {
	resp, err := c.doRequest(ctx, prompt)
	if err != nil {
		if isRetryable(err) {
			delay := retryDelay(err)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return "", ctx.Err()
			}
			resp, err = c.doRequest(ctx, prompt)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}
	return resp, nil
}

func (c *HTTPClient) doRequest(ctx context.Context, prompt string) (string, error) {
	maxTokens := c.config.MaxTokens
	if maxTokens == 0 {
		maxTokens = 1024
	}

	reqBody := chatRequest{
		Model:       c.config.Model,
		Messages:    []chatMessage{{Role: "user", Content: prompt}},
		MaxTokens:   maxTokens,
		Temperature: c.config.Temperature,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.Endpoint, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := resp.Header.Get("Retry-After")
		return "", &retryableError{
			statusCode: resp.StatusCode,
			retryAfter: retryAfter,
			body:       string(respBody),
		}
	}
	if resp.StatusCode >= 500 {
		return "", &retryableError{
			statusCode: resp.StatusCode,
			body:       string(respBody),
		}
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("LLM API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var chatResp chatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}
	if len(chatResp.Choices) == 0 {
		return "", fmt.Errorf("LLM returned empty choices")
	}
	return chatResp.Choices[0].Message.Content, nil
}

type retryableError struct {
	statusCode int
	retryAfter string
	body       string
}

func (e *retryableError) Error() string {
	return fmt.Sprintf("LLM API returned %d: %s", e.statusCode, e.body)
}

func isRetryable(err error) bool {
	_, ok := err.(*retryableError)
	return ok
}

func retryDelay(err error) time.Duration {
	re, ok := err.(*retryableError)
	if !ok {
		return 2 * time.Second
	}
	if re.retryAfter != "" {
		if secs, err := strconv.Atoi(re.retryAfter); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	if re.statusCode == http.StatusTooManyRequests {
		return 5 * time.Second
	}
	return 2 * time.Second
}
