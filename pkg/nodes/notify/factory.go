// Package notify provides a node that sends a message to Slack or by email.
package notify

import (
	"context"

	"github.com/dukex/operion/pkg/protocol"
)

type NotifyNodeFactory struct{}

func NewNotifyNodeFactory() protocol.NodeFactory {
	return &NotifyNodeFactory{}
}

func (f *NotifyNodeFactory) Create(_ context.Context, id string, config map[string]any) (protocol.Node, error) {
	return NewNotifyNode(id, config)
}

func (f *NotifyNodeFactory) ID() string {
	return "notify"
}

func (f *NotifyNodeFactory) Name() string {
	return "Notify"
}

func (f *NotifyNodeFactory) Description() string {
	return "Sends a templated message to a Slack incoming webhook or by email over SMTP"
}

func (f *NotifyNodeFactory) Schema() map[string]any {
	const typeKey = "type"

	text := func(description string) map[string]any {
		return map[string]any{typeKey: "string", "description": description}
	}

	return map[string]any{
		typeKey: "object",
		"properties": map[string]any{
			keyChannel:          map[string]any{typeKey: "string", "enum": []string{ChannelSlack, ChannelEmail}},
			keyMessage:          text("Message text, or email body. Supports templating, e.g. Welcome, {{.trigger_data.body.name}}"),
			keyWebhookURLEnv:    text("Slack: environment variable that holds the incoming webhook URL"),
			"smtp_host":         text("Email: SMTP host"),
			keySMTPPort:         map[string]any{typeKey: "integer", "default": defaultSMTPPort},
			"smtp_username_env": text("Email: environment variable that holds the SMTP username"),
			"smtp_password_env": text("Email: environment variable that holds the SMTP password"),
			keyFrom:             text("Email: sender address"),
			keyTo:               text("Email: comma-separated recipients. Supports templating, e.g. {{.trigger_data.body.manager_email}}"),
			keySubject:          text("Email: subject. Supports templating"),
		},
		"required": []string{keyChannel, keyMessage},
		"examples": []map[string]any{
			{keyChannel: ChannelSlack, keyMessage: "Bem-vinda, {{.trigger_data.body.name}}", keyWebhookURLEnv: "SLACK_WEBHOOK_URL"},
			{
				keyChannel: ChannelEmail, keyMessage: "{{.trigger_data.body.name}} joined today",
				"smtp_host": "smtp.example.com", keyFrom: "operion@example.com",
				keyTo: "{{.trigger_data.body.manager_email}}", keySubject: "New hire",
				"smtp_username_env": "SMTP_USERNAME", "smtp_password_env": "SMTP_PASSWORD",
			},
		},
	}
}
