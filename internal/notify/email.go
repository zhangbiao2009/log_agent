package notify

import (
	"bytes"
	"context"
	"fmt"
	"github.com/zhangbiao2009/log_agent/internal/core"
	"html/template"
	"net/smtp"
	"strings"
)

// EmailConfig holds SMTP settings for the email notifier.
type EmailConfig struct {
	Host       string
	Port       int
	Username   string
	Password   string
	From       string
	Recipients []string
	UseTLS     bool
}

// sendMailFunc matches net/smtp.SendMail signature for testing.
type sendMailFunc func(addr string, a smtp.Auth, from string, to []string, msg []byte) error

// EmailNotifier sends incident notifications via SMTP email.
type EmailNotifier struct {
	cfg      EmailConfig
	sendMail sendMailFunc
}

// NewEmailNotifier creates an EmailNotifier with SMTP delivery.
func NewEmailNotifier(cfg EmailConfig) *EmailNotifier {
	if cfg.Port == 0 {
		cfg.Port = 587
	}
	return &EmailNotifier{
		cfg:      cfg,
		sendMail: smtp.SendMail,
	}
}

func (e *EmailNotifier) Name() string { return "email" }

func (e *EmailNotifier) Send(_ context.Context, incident core.Incident) error {
	if len(e.cfg.Recipients) == 0 {
		return fmt.Errorf("email: no recipients configured")
	}

	subject := e.buildSubject(incident)
	body, err := e.buildBody(incident)
	if err != nil {
		return fmt.Errorf("email: render body: %w", err)
	}

	msg := e.buildMessage(subject, body)

	addr := fmt.Sprintf("%s:%d", e.cfg.Host, e.cfg.Port)
	var auth smtp.Auth
	if e.cfg.Username != "" {
		auth = smtp.PlainAuth("", e.cfg.Username, e.cfg.Password, e.cfg.Host)
	}

	if err := e.sendMail(addr, auth, e.cfg.From, e.cfg.Recipients, msg); err != nil {
		return fmt.Errorf("email: send: %w", err)
	}
	return nil
}

func (e *EmailNotifier) buildSubject(inc core.Incident) string {
	sev := inc.Severity
	if sev == "" {
		sev = "INFO"
	}

	service := inc.RootService
	if service == "" && len(inc.Services) > 0 {
		service = inc.Services[0]
	}

	switch inc.EventType {
	case "resolved":
		return fmt.Sprintf("[%s] INCIDENT RESOLVED — %s", sev, service)
	case "updated":
		return fmt.Sprintf("[%s] INCIDENT UPDATE — %s", sev, service)
	default:
		return fmt.Sprintf("[%s] INCIDENT OPENED — %s", sev, service)
	}
}

func (e *EmailNotifier) buildMessage(subject, htmlBody string) []byte {
	var buf bytes.Buffer
	buf.WriteString("From: " + e.cfg.From + "\r\n")
	buf.WriteString("To: " + strings.Join(e.cfg.Recipients, ", ") + "\r\n")
	buf.WriteString("Subject: " + subject + "\r\n")
	buf.WriteString("MIME-Version: 1.0\r\n")
	buf.WriteString("Content-Type: text/html; charset=\"UTF-8\"\r\n")
	buf.WriteString("\r\n")
	buf.WriteString(htmlBody)
	return buf.Bytes()
}

var emailTmpl = template.Must(template.New("email").Parse(`<!DOCTYPE html>
<html><body style="font-family:sans-serif;font-size:14px;">
<h2 style="color:{{.Color}};">{{.Title}}</h2>
{{if .RootService}}<p><strong>Root cause:</strong> {{.RootService}}</p>{{end}}
{{if .Services}}<p><strong>Affected services:</strong> {{.Services}}</p>{{end}}
{{if .Chain}}<p><strong>Dependency chain:</strong> {{.Chain}}</p>{{end}}
{{if .Duration}}<p><strong>Duration:</strong> {{.Duration}}</p>{{end}}
{{if .Diagnosis}}<h3>Diagnosis</h3><p>{{.Diagnosis}}</p>{{end}}
{{if .Suggestions}}<h3>Suggested Actions</h3><ol>{{range .Suggestions}}<li>{{.}}</li>{{end}}</ol>{{end}}
{{range .Alerts}}<h3>[{{.Service}}] {{.Count}}× {{.Level}}</h3>
{{range .Patterns}}<p><code>{{.Template}}</code> ({{.Count}}×)</p>{{end}}
{{end}}
</body></html>`))

type emailData struct {
	Color       string
	Title       string
	RootService string
	Services    string
	Chain       string
	Duration    string
	Diagnosis   string
	Suggestions []string
	Alerts      []core.Alert
}

func (e *EmailNotifier) buildBody(inc core.Incident) (string, error) {
	data := emailData{
		Color:       severityColor(inc.Severity),
		Title:       e.buildSubject(inc),
		RootService: inc.RootService,
		Services:    strings.Join(inc.Services, ", "),
		Chain:       strings.Join(inc.DepChain, " → "),
		Diagnosis:   inc.Diagnosis,
		Suggestions: inc.Suggestions,
		Alerts:      inc.Alerts,
	}
	if inc.Duration > 0 {
		data.Duration = inc.Duration.String()
	}

	var buf bytes.Buffer
	if err := emailTmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func severityColor(sev string) string {
	switch sev {
	case "P1":
		return "#d32f2f"
	case "P2":
		return "#f9a825"
	case "P3":
		return "#1976d2"
	default:
		return "#333333"
	}
}
