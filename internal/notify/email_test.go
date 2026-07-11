package notify

import (
	"context"
	"fmt"
	"github.com/zhangbiao2009/log_agent/internal/core"
	"net/smtp"
	"strings"
	"testing"
	"time"
)

func makeTestIncident(eventType string) core.Incident {
	inc := core.Incident{
		ID:          "test-123",
		Services:    []string{"order-service", "payment-service", "bank-gateway"},
		RootService: "bank-gateway",
		DepChain:    []string{"bank-gateway", "payment-service", "order-service"},
		Alerts: []core.Alert{
			{Service: "bank-gateway", Level: "ERROR", Count: 200, Window: time.Minute},
		},
		OpenedAt:    time.Now(),
		Window:      2 * time.Minute,
		Diagnosis:   "bank-gateway stopped responding after deploy",
		Severity:    "P1",
		Suggestions: []string{"Rollback bank-gateway to v2.3.0", "Check DB connections"},
		Status:      core.StatusOpen,
		EventType:   eventType,
	}
	if eventType == "resolved" {
		inc.Status = core.StatusResolved
		inc.Duration = 5 * time.Minute
	}
	if eventType == "updated" {
		inc.Status = core.StatusOngoing
	}
	return inc
}

// captureSendMail returns a sendMailFunc that captures the call args.
type capturedEmail struct {
	addr string
	from string
	to   []string
	msg  []byte
}

func captureSendMail(captured *capturedEmail, err error) sendMailFunc {
	return func(addr string, _ smtp.Auth, from string, to []string, msg []byte) error {
		captured.addr = addr
		captured.from = from
		captured.to = to
		captured.msg = make([]byte, len(msg))
		copy(captured.msg, msg)
		return err
	}
}

func TestEmail_Name(t *testing.T) {
	e := NewEmailNotifier(EmailConfig{})
	if e.Name() != "email" {
		t.Errorf("Name() = %q, want %q", e.Name(), "email")
	}
}

func TestEmail_OpenedIncident_SubjectAndBody(t *testing.T) {
	var cap capturedEmail
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", Port: 587,
		From: "alerts@test.com", Recipients: []string{"oncall@test.com"},
	})
	e.sendMail = captureSendMail(&cap, nil)

	inc := makeTestIncident("opened")
	if err := e.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	msg := string(cap.msg)
	if !strings.Contains(msg, "Subject: [P1] INCIDENT OPENED") {
		t.Errorf("subject missing severity/opened, got: %s", firstLine(msg, "Subject:"))
	}
	if !strings.Contains(msg, "bank-gateway") {
		t.Error("body missing root service")
	}
	if !strings.Contains(msg, "Rollback bank-gateway to v2.3.0") {
		t.Error("body missing suggestions")
	}
	if !strings.Contains(msg, "Content-Type: text/html") {
		t.Error("missing HTML content type")
	}
}

func TestEmail_ResolvedIncident_SubjectAndBody(t *testing.T) {
	var cap capturedEmail
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", From: "a@b.com", Recipients: []string{"c@d.com"},
	})
	e.sendMail = captureSendMail(&cap, nil)

	inc := makeTestIncident("resolved")
	if err := e.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	msg := string(cap.msg)
	if !strings.Contains(msg, "RESOLVED") {
		t.Errorf("subject missing RESOLVED, got: %s", firstLine(msg, "Subject:"))
	}
	if !strings.Contains(msg, "5m0s") {
		t.Error("body missing duration")
	}
}

func TestEmail_UpdatedIncident_SubjectAndBody(t *testing.T) {
	var cap capturedEmail
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", From: "a@b.com", Recipients: []string{"c@d.com"},
	})
	e.sendMail = captureSendMail(&cap, nil)

	inc := makeTestIncident("updated")
	if err := e.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	msg := string(cap.msg)
	if !strings.Contains(msg, "UPDATE") {
		t.Errorf("subject missing UPDATE")
	}
}

func TestEmail_SingleAlert_Format(t *testing.T) {
	var cap capturedEmail
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", From: "a@b.com", Recipients: []string{"c@d.com"},
	})
	e.sendMail = captureSendMail(&cap, nil)

	inc := core.Incident{
		ID:        "single-1",
		Services:  []string{"myapp"},
		Alerts:    []core.Alert{{Service: "myapp", Level: "ERROR", Count: 10, Window: time.Minute}},
		Severity:  "P3",
		EventType: "opened",
		Status:    core.StatusOpen,
	}
	if err := e.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	msg := string(cap.msg)
	if !strings.Contains(msg, "myapp") {
		t.Error("body missing service name")
	}
}

func TestEmail_HTMLEscaping(t *testing.T) {
	var cap capturedEmail
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", From: "a@b.com", Recipients: []string{"c@d.com"},
	})
	e.sendMail = captureSendMail(&cap, nil)

	inc := makeTestIncident("opened")
	inc.Diagnosis = "<script>alert('xss')</script>"
	inc.RootService = "svc<injected>"

	if err := e.Send(context.Background(), inc); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	msg := string(cap.msg)
	if strings.Contains(msg, "<script>") {
		t.Error("HTML not escaped — XSS vulnerability")
	}
	if !strings.Contains(msg, "&lt;script&gt;") {
		t.Error("expected HTML-escaped diagnosis")
	}
}

func TestEmail_SendError_Propagated(t *testing.T) {
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", From: "a@b.com", Recipients: []string{"c@d.com"},
	})
	e.sendMail = captureSendMail(&capturedEmail{}, fmt.Errorf("connection refused"))

	err := e.Send(context.Background(), makeTestIncident("opened"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error = %v, want connection refused", err)
	}
}

func TestEmail_MultipleRecipients(t *testing.T) {
	var cap capturedEmail
	recipients := []string{"a@test.com", "b@test.com", "c@test.com"}
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", From: "alerts@test.com", Recipients: recipients,
	})
	e.sendMail = captureSendMail(&cap, nil)

	if err := e.Send(context.Background(), makeTestIncident("opened")); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if len(cap.to) != 3 {
		t.Errorf("expected 3 recipients, got %d", len(cap.to))
	}
}

func TestEmail_EmptyRecipients(t *testing.T) {
	e := NewEmailNotifier(EmailConfig{Host: "smtp.test.com", From: "a@b.com"})
	err := e.Send(context.Background(), makeTestIncident("opened"))
	if err == nil {
		t.Fatal("expected error for empty recipients")
	}
}

func TestEmail_TemplateRenders_WithAllFields(t *testing.T) {
	var cap capturedEmail
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", From: "a@b.com", Recipients: []string{"c@d.com"},
	})
	e.sendMail = captureSendMail(&cap, nil)

	inc := makeTestIncident("opened")
	if err := e.Send(context.Background(), inc); err != nil {
		t.Fatalf("template render failed: %v", err)
	}
	if len(cap.msg) == 0 {
		t.Error("empty message")
	}
}

func TestEmail_TemplateRenders_MinimalFields(t *testing.T) {
	var cap capturedEmail
	e := NewEmailNotifier(EmailConfig{
		Host: "smtp.test.com", From: "a@b.com", Recipients: []string{"c@d.com"},
	})
	e.sendMail = captureSendMail(&cap, nil)

	inc := core.Incident{
		ID:        "min-1",
		Services:  []string{"svc"},
		Alerts:    []core.Alert{{Service: "svc", Level: "ERROR", Count: 1, Window: time.Minute}},
		EventType: "opened",
		Status:    core.StatusOpen,
	}
	if err := e.Send(context.Background(), inc); err != nil {
		t.Fatalf("template render with minimal fields failed: %v", err)
	}
}

// firstLine returns the first line in msg that contains prefix.
func firstLine(msg, prefix string) string {
	for _, line := range strings.Split(msg, "\n") {
		if strings.Contains(line, prefix) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}
