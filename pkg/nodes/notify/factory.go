// Package notify provides a node that sends a message to Slack or by email.
package notify

import (
	"context"

	"github.com/dukex/operion/pkg/protocol"
)

type NotifyNodeFactory struct {
	mail SMTP
}

func NewNotifyNodeFactory(mail SMTP) protocol.NodeFactory {
	return &NotifyNodeFactory{mail: mail}
}

func (f *NotifyNodeFactory) Create(_ context.Context, id string, config map[string]any) (protocol.Node, error) {
	return NewNotifyNode(id, config, f.mail)
}

func (f *NotifyNodeFactory) ID() string {
	return "notify"
}

func (f *NotifyNodeFactory) Name() string {
	return "Notify"
}

func (f *NotifyNodeFactory) Description() string {
	return "Sends a templated message to a Slack incoming webhook, or by email through the server's SMTP"
}

func (f *NotifyNodeFactory) Schema() map[string]any {
	const typeKey = "type"

	text := func(description string) map[string]any {
		return map[string]any{typeKey: "string", "description": description}
	}

	return map[string]any{
		typeKey: "object",
		"properties": map[string]any{
			keyChannel:    map[string]any{typeKey: "string", "enum": []string{ChannelSlack, ChannelEmail}},
			keyMessage:    text("Message text, or email body. Supports templating, e.g. Welcome, {{.trigger_data.body.name}}"),
			keyWebhookURL: text("Slack: incoming webhook URL"),
			keyTo:         text("Email: comma-separated recipients. Supports templating, e.g. {{.trigger_data.body.manager_email}}"),
			keySubject:    text("Email: subject. Supports templating"),
		},
		"required": []string{keyChannel, keyMessage},
		"examples": []map[string]any{
			{keyChannel: ChannelSlack, keyMessage: "Bem-vinda, {{.trigger_data.body.name}}", keyWebhookURL: "https://hooks.slack.com/services/T000/B000/XXXX"},
			{
				keyChannel: ChannelEmail, keyMessage: "{{.trigger_data.body.name}} joined today",
				keyTo: "{{.trigger_data.body.manager_email}}", keySubject: "New hire",
			},
		},
	}
}
