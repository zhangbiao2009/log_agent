package pattern

import (
	"strings"
	"testing"
)

func TestPreprocess_PlainText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"ip replaced", "connection to 192.168.1.1 timed out", "connection to <IP> timed out"},
		{"port number", "connecting to 10.0.0.1:8080", "connecting to <IP>:<NUM>"},
		{"number replaced", "processed 42 items", "processed <NUM> items"},
		{"uuid replaced", "request 550e8400-e29b-41d4-a716-446655440000 ok", "request <UUID> ok"},
		{"long hex replaced", "span id abcdef1234567890 ok", "span id <HEX> ok"},
		{"no variables", "server started", "server started"},
		{"empty string", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Preprocess(tc.in)
			if got != tc.want {
				t.Errorf("Preprocess(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestPreprocess_JSONMsgField(t *testing.T) {
	in := "{\"msg\":\"connection timeout\",\"level\":\"error\"}"
	got := Preprocess(in)
	if !strings.Contains(got, "connection timeout") {
		t.Errorf("expected message text, got: %q", got)
	}
	if strings.Contains(got, "\"msg\"") {
		t.Errorf("raw JSON keys should not appear, got: %q", got)
	}
}

func TestPreprocess_JSONMessageField(t *testing.T) {
	in := "{\"message\":\"disk full\",\"level\":\"error\"}"
	got := Preprocess(in)
	if !strings.Contains(got, "disk full") {
		t.Errorf("expected message text, got: %q", got)
	}
}

func TestPreprocess_JSONMsgPreferredOverMessage(t *testing.T) {
	in := "{\"msg\":\"primary\",\"message\":\"secondary\"}"
	got := Preprocess(in)
	if !strings.Contains(got, "primary") {
		t.Errorf("expected 'primary', got: %q", got)
	}
	if strings.Contains(got, "secondary") {
		t.Errorf("expected 'secondary' to be excluded, got: %q", got)
	}
}

func TestPreprocess_JSONErrField(t *testing.T) {
	in := "{\"msg\":\"request failed\",\"err\":\"connection refused\"}"
	got := Preprocess(in)
	if !strings.Contains(got, "connection refused") {
		t.Errorf("expected err value, got: %q", got)
	}
}

func TestPreprocess_JSONMethodField(t *testing.T) {
	in := "{\"msg\":\"slow call\",\"method\":\"GetUser\"}"
	got := Preprocess(in)
	if !strings.Contains(got, "GetUser") {
		t.Errorf("expected method value, got: %q", got)
	}
}

func TestPreprocess_JSONNoTargetFields(t *testing.T) {
	in := "{\"level\":\"info\",\"service\":\"svc\"}"
	got := Preprocess(in)
	// Should return raw unchanged since no msg/method/err fields.
	if got != in {
		t.Errorf("expected raw unchanged, got: %q", got)
	}
}

func TestPreprocess_NonJSON(t *testing.T) {
	in := "plain text log line"
	got := Preprocess(in)
	if got != in {
		t.Errorf("expected raw unchanged, got: %q", got)
	}
}

func TestPreprocess_JSONWithIPAndNumber(t *testing.T) {
	// The "err" field "timeout after 30s" uses "30s" not a bare number,
	// so <NUM> won't appear; but the IP in the message should be replaced.
	in := "{\"msg\":\"connection to 192.168.1.1 failed\",\"err\":\"timeout after 30s\"}"
	got := Preprocess(in)
	if !strings.Contains(got, "<IP>") {
		t.Errorf("expected <IP> in output, got: %q", got)
	}
	if !strings.Contains(got, "connection to") {
		t.Errorf("expected message text in output, got: %q", got)
	}
	// "30s" should remain unchanged (not a standalone number)
	if strings.Contains(got, "<NUM>") && !strings.Contains(got, "30") {
		t.Errorf("unexpected <NUM> replacement for embedded digit, got: %q", got)
	}
}

func TestPreprocess_StandaloneNumber(t *testing.T) {
	in := "{\"msg\":\"processed 42 requests\"}"
	got := Preprocess(in)
	if !strings.Contains(got, "<NUM>") {
		t.Errorf("expected <NUM> for standalone number, got: %q", got)
	}
}
