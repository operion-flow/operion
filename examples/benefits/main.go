package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	maxBody          = 1 << 20
	maxClockSkew     = 5 * time.Minute
	autoApproveLimit = 500
	approvalHours    = 72
	portApproved     = "approved"
	portError        = "error"
	amountKey        = "amount"
)

type request struct {
	ExecutionID string         `json:"execution_id"`
	NodeID      string         `json:"node_id"`
	Config      map[string]any `json:"config"`
	CallbackURL string         `json:"callback_url"`
}

type answer struct {
	Port string         `json:"port"`
	Data map[string]any `json:"data"`
}

type plugin struct {
	secret string
	client *http.Client
	review time.Duration
	logger *slog.Logger
}

func main() {
	addr := ":" + envOr("PORT", "8080")
	p := plugin{secret: os.Getenv("OPERION_SECRET"), client: &http.Client{Timeout: 10 * time.Second}, review: 5 * time.Second, logger: slog.Default()}

	p.logger.Info("benefits node listening", "addr", addr, "signed", p.secret != "")

	server := &http.Server{Addr: addr, Handler: p, ReadHeaderTimeout: 10 * time.Second}
	if err := server.ListenAndServe(); err != nil {
		p.logger.Error("benefits node stopped", "error", err)
		os.Exit(1)
	}
}

func (p plugin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	if p.secret != "" && !verified(p.secret, r.Header, body, time.Now()) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)

		return
	}

	var call request
	if err := json.Unmarshal(body, &call); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	amount, err := strconv.ParseFloat(fmt.Sprint(call.Config[amountKey]), 64)
	if err != nil || amount <= 0 {
		p.reply(w, http.StatusOK, failure("amount must be a positive number"))

		return
	}

	grant := answer{Port: portApproved, Data: map[string]any{"benefit_id": call.ExecutionID + "/" + call.NodeID, amountKey: amount}}

	if amount <= autoApproveLimit {
		p.reply(w, http.StatusOK, grant)

		return
	}

	if call.CallbackURL == "" {
		p.reply(w, http.StatusOK, failure("amounts above the limit need a callback_url"))

		return
	}

	p.reply(w, http.StatusAccepted, map[string]any{"timeout_seconds": approvalHours * 3600})

	go p.answerLater(context.WithoutCancel(r.Context()), call.CallbackURL, grant)
}

func (p plugin) answerLater(ctx context.Context, callbackURL string, grant answer) {
	time.Sleep(p.review)

	body, err := json.Marshal(grant)
	if err != nil {
		p.logger.Error("encode answer", "error", err)

		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		p.logger.Error("build callback", "error", err)

		return
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Error("callback failed", "error", err)

		return
	}

	_ = resp.Body.Close()

	p.logger.Info("answered", "status", resp.StatusCode)
}

func verified(secret string, header http.Header, body []byte, now time.Time) bool {
	timestamp := header.Get("X-Operion-Timestamp")

	sent, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || now.Sub(time.Unix(sent, 0)).Abs() > maxClockSkew {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)

	return hmac.Equal([]byte(header.Get("X-Operion-Signature")), []byte("sha256="+hex.EncodeToString(mac.Sum(nil))))
}

func failure(message string) answer {
	return answer{Port: portError, Data: map[string]any{"error": message}}
}

func (p plugin) reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		p.logger.Error("write answer", "error", err)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}
