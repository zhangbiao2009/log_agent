// Mock LLM server for Phase 5 diagnosis demo.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
}

type chatChoice struct {
	Message chatMessage `json:"message"`
}

func handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	var req chatRequest
	json.Unmarshal(body, &req)

	prompt := ""
	if len(req.Messages) > 0 {
		prompt = req.Messages[0].Content
	}

	log.Printf("received prompt (%d chars), generating diagnosis...", len(prompt))

	diagnosis := generateDiagnosis(prompt)

	resp := chatResponse{
		Choices: []chatChoice{
			{Message: chatMessage{Role: "assistant", Content: diagnosis}},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
	log.Printf("sent diagnosis response")
}

func generateDiagnosis(prompt string) string {
	hasBankGW := strings.Contains(prompt, "bank-gateway")
	hasPayment := strings.Contains(prompt, "payment-service")
	hasOrder := strings.Contains(prompt, "order-service")

	if hasBankGW && hasPayment && hasOrder {
		return "SEVERITY: P1\nDIAGNOSIS: bank-gateway is experiencing connection resets (connection reset by peer) when calling the external bank API at bank-api.example.com. This has caused a cascading failure: payment-service cannot reach bank-gateway on port 443 (connection refused), and order-service is receiving 503 errors from payment-service. The root cause is likely a network issue or TLS certificate expiry on the bank-gateway outbound connection.\nSUGGESTIONS:\n- Rollback bank-gateway to the last known working version if a recent deploy occurred\n- Check TLS certificate expiry on bank-gateway connection to bank-api.example.com\n- Verify network connectivity and firewall rules between bank-gateway and the external bank API\n- Monitor bank-gateway health checks after remediation to confirm recovery"
	}

	if hasBankGW {
		return fmt.Sprintf("SEVERITY: P2\nDIAGNOSIS: bank-gateway health checks are failing with i/o timeout errors against bank-api.example.com.\nSUGGESTIONS:\n- Check external bank API status page for known outages\n- Test connectivity from bank-gateway host to bank-api.example.com")
	}

	return "SEVERITY: P2\nDIAGNOSIS: Multiple error patterns detected across services.\nSUGGESTIONS:\n- Check recent deployments for the affected services\n- Review infrastructure metrics for anomalies"
}

func main() {
	http.HandleFunc("/v1/chat/completions", handler)
	addr := ":9999"
	log.Printf("mock LLM server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
