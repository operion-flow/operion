package notify

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/smtp"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
)

const (
	InputPortMain     = "main"
	OutputPortSuccess = "success"
	OutputPortError   = "error"

	ChannelSlack = "slack"
	ChannelEmail = "email"

	defaultSMTPPort = 587

	keyChannel       = "channel"
	keyMessage       = "message"
	keyWebhookURLEnv = "webhook_url_env"
	keySMTPPort      = "smtp_port"
	keyFrom          = "from"
	keyTo            = "to"
	keySubject       = "subject"
)

type config struct {
	channel       string
	message       string
	webhookURLEnv string
	smtpHost      string
	smtpPort      int
	usernameEnv   string
	passwordEnv   string
	from          string
	to            string
	subject       string
}

type NotifyNode struct {
	id       string
	config   config
	client   *http.Client
	sendMail func(addr string, auth smtp.Auth, from string, to []string, msg []byte) error
}

func NewNotifyNode(id string, raw map[string]any) (*NotifyNode, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}

	return &NotifyNode{
		id:       id,
		config:   cfg,
		client:   &http.Client{Timeout: 10 * time.Second},
		sendMail: smtp.SendMail,
	}, nil
}

func parseConfig(raw map[string]any) (config, error) {
	cfg := config{smtpPort: defaultSMTPPort}

	cfg.channel, _ = raw[keyChannel].(string)
	cfg.message, _ = raw[keyMessage].(string)
	cfg.webhookURLEnv, _ = raw[keyWebhookURLEnv].(string)
	cfg.smtpHost, _ = raw["smtp_host"].(string)
	cfg.usernameEnv, _ = raw["smtp_username_env"].(string)
	cfg.passwordEnv, _ = raw["smtp_password_env"].(string)
	cfg.from, _ = raw[keyFrom].(string)
	cfg.to, _ = raw[keyTo].(string)
	cfg.subject, _ = raw[keySubject].(string)

	if cfg.message == "" {
		return cfg, errors.New("missing required field 'message'")
	}

	if port, ok := raw[keySMTPPort]; ok {
		parsed, err := parsePort(port)
		if err != nil {
			return cfg, err
		}

		cfg.smtpPort = parsed
	}

	switch cfg.channel {
	case ChannelSlack:
		if cfg.webhookURLEnv == "" {
			return cfg, errors.New("slack needs 'webhook_url_env', the environment variable holding the webhook URL")
		}
	case ChannelEmail:
		if cfg.smtpHost == "" || cfg.from == "" || cfg.to == "" {
			return cfg, errors.New("email needs 'smtp_host', 'from' and 'to'")
		}
	default:
		return cfg, fmt.Errorf("'channel' must be %q or %q", ChannelSlack, ChannelEmail)
	}

	return cfg, nil
}

func parsePort(value any) (int, error) {
	switch port := value.(type) {
	case float64:
		return int(port), nil
	case int:
		return port, nil
	case string:
		parsed, err := strconv.Atoi(port)
		if err != nil {
			return 0, fmt.Errorf("invalid 'smtp_port' %q", port)
		}

		return parsed, nil
	default:
		return 0, fmt.Errorf("invalid 'smtp_port' %v", value)
	}
}

func (n *NotifyNode) ID() string {
	return n.id
}

func (n *NotifyNode) Type() string {
	return "notify"
}

func (n *NotifyNode) Execute(ctx models.ExecutionContext, _ map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	message, err := render(n.config.message, &ctx)
	if err != nil {
		return n.failure(err), nil
	}

	data := map[string]any{keyChannel: n.config.channel, keyMessage: message}

	if n.config.channel == ChannelSlack {
		err = n.sendSlack(message)
	} else {
		data[keyTo], err = n.sendEmail(&ctx, message)
	}

	if err != nil {
		return n.failure(err), nil
	}

	return map[string]models.NodeResult{
		OutputPortSuccess: {NodeID: n.id, Data: data, Status: string(models.NodeStatusSuccess)},
	}, nil
}

func (n *NotifyNode) sendSlack(message string) error {
	url := os.Getenv(n.config.webhookURLEnv)
	if url == "" {
		return fmt.Errorf("environment variable %s is empty", n.config.webhookURLEnv)
	}

	body, err := json.Marshal(map[string]string{"text": message})
	if err != nil {
		return err
	}

	resp, err := n.client.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx,gosec // nodes get no context yet, and the URL comes from an environment variable the operator sets
	if err != nil {
		return fmt.Errorf("slack request failed: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("slack answered %d", resp.StatusCode)
	}

	return nil
}

func (n *NotifyNode) sendEmail(ctx *models.ExecutionContext, body string) ([]string, error) {
	to, err := render(n.config.to, ctx)
	if err != nil {
		return nil, err
	}

	subject, err := render(n.config.subject, ctx)
	if err != nil {
		return nil, err
	}

	for _, header := range []string{n.config.from, to, subject} {
		if strings.ContainsAny(header, "\r\n") {
			return nil, errors.New("email headers cannot contain line breaks")
		}
	}

	recipients := strings.Split(to, ",")
	for i := range recipients {
		recipients[i] = strings.TrimSpace(recipients[i])
	}

	var auth smtp.Auth
	if n.config.usernameEnv != "" {
		auth = smtp.PlainAuth("", os.Getenv(n.config.usernameEnv), os.Getenv(n.config.passwordEnv), n.config.smtpHost)
	}

	msg := "From: " + n.config.from + "\r\n" +
		"To: " + strings.Join(recipients, ", ") + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"Content-Type: text/plain; charset=UTF-8\r\n\r\n" +
		body

	addr := net.JoinHostPort(n.config.smtpHost, strconv.Itoa(n.config.smtpPort))
	if err := n.sendMail(addr, auth, n.config.from, recipients, []byte(msg)); err != nil {
		return nil, fmt.Errorf("email delivery failed: %w", err)
	}

	return recipients, nil
}

func render(value string, ctx *models.ExecutionContext) (string, error) {
	rendered, err := template.RenderWithContext(value, ctx)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v", rendered), nil
}

func (n *NotifyNode) failure(err error) map[string]models.NodeResult {
	return map[string]models.NodeResult{
		OutputPortError: {
			NodeID: n.id,
			Data:   map[string]any{"error": err.Error(), "success": false},
			Status: string(models.NodeStatusError),
			Error:  err.Error(),
		},
	}
}

func (n *NotifyNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{Port: models.Port{ID: models.MakePortID(n.id, InputPortMain), NodeID: n.id, Name: InputPortMain, Description: "Sends the notification"}},
	}
}

func (n *NotifyNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{
		{Port: models.Port{ID: models.MakePortID(n.id, OutputPortSuccess), NodeID: n.id, Name: OutputPortSuccess, Description: "Channel, rendered message and, for email, the recipients"}},
		{Port: models.Port{ID: models.MakePortID(n.id, OutputPortError), NodeID: n.id, Name: OutputPortError, Description: "Why the notification was not sent"}},
	}
}

func (n *NotifyNode) Validate(raw map[string]any) error {
	_, err := parseConfig(raw)

	return err
}
