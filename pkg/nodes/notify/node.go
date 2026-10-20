package notify

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
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

	keyChannel    = "channel"
	keyMessage    = "message"
	keyWebhookURL = "webhook_url"
	keyTo         = "to"
	keySubject    = "subject"
)

var (
	errWebhookURL         = errors.New("slack needs 'webhook_url', the http(s) URL of the incoming webhook")
	errEmailNotConfigured = errors.New("email is not configured on this server, the operator sets --smtp-host and --smtp-from")
)

// SMTP is the mail server the operator configures for email notifications.
type SMTP struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

type config struct {
	channel    string
	message    string
	webhookURL string
	to         string
	subject    string
}

type NotifyNode struct {
	id       string
	config   config
	mail     SMTP
	client   *http.Client
	sendMail func(addr string, auth smtp.Auth, from string, to []string, msg []byte) error
}

func NewNotifyNode(id string, raw map[string]any, mail SMTP) (*NotifyNode, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}

	return &NotifyNode{
		id:       id,
		config:   cfg,
		mail:     mail,
		client:   &http.Client{Timeout: 10 * time.Second},
		sendMail: smtp.SendMail,
	}, nil
}

func parseConfig(raw map[string]any) (config, error) {
	var cfg config

	cfg.channel, _ = raw[keyChannel].(string)
	cfg.message, _ = raw[keyMessage].(string)
	cfg.webhookURL, _ = raw[keyWebhookURL].(string)
	cfg.to, _ = raw[keyTo].(string)
	cfg.subject, _ = raw[keySubject].(string)

	if cfg.message == "" {
		return cfg, errors.New("missing required field 'message'")
	}

	switch cfg.channel {
	case ChannelSlack:
		if !isHTTPURL(cfg.webhookURL) {
			return cfg, errWebhookURL
		}
	case ChannelEmail:
		if cfg.to == "" {
			return cfg, errors.New("email needs 'to'")
		}
	default:
		return cfg, fmt.Errorf("'channel' must be %q or %q", ChannelSlack, ChannelEmail)
	}

	return cfg, nil
}

func isHTTPURL(raw string) bool {
	parsed, err := url.Parse(raw)

	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
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
	body, err := json.Marshal(map[string]string{"text": message})
	if err != nil {
		return err
	}

	resp, err := n.client.Post(n.config.webhookURL, "application/json", bytes.NewReader(body)) //nolint:noctx // nodes get no context yet
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
	if n.mail.Host == "" || n.mail.From == "" {
		return nil, errEmailNotConfigured
	}

	to, err := render(n.config.to, ctx)
	if err != nil {
		return nil, err
	}

	subject, err := render(n.config.subject, ctx)
	if err != nil {
		return nil, err
	}

	for _, header := range []string{n.mail.From, to, subject} {
		if strings.ContainsAny(header, "\r\n") {
			return nil, errors.New("email headers cannot contain line breaks")
		}
	}

	recipients := strings.Split(to, ",")
	for i := range recipients {
		recipients[i] = strings.TrimSpace(recipients[i])
	}

	var auth smtp.Auth
	if n.mail.Username != "" {
		auth = smtp.PlainAuth("", n.mail.Username, n.mail.Password, n.mail.Host)
	}

	msg := "From: " + n.mail.From + "\r\n" +
		"To: " + strings.Join(recipients, ", ") + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"Content-Type: text/plain; charset=UTF-8\r\n\r\n" +
		body

	addr := net.JoinHostPort(n.mail.Host, strconv.Itoa(n.mail.Port))
	if err := n.sendMail(addr, auth, n.mail.From, recipients, []byte(msg)); err != nil {
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
