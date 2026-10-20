package notify

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	slackURL       = "https://hooks.slack.example.com/services/T000"
	brokenTemplate = "{{.trigger_data"
)

func mailServer() SMTP {
	return SMTP{Host: "smtp.example.com", Port: 2525, From: "operion@example.com"}
}

func execution() models.ExecutionContext {
	return models.ExecutionContext{
		ID:          "exec",
		TriggerData: map[string]any{"body": map[string]any{"name": "Ana", "manager": "lead@example.com, hr@example.com"}},
	}
}

func run(t *testing.T, node *NotifyNode) (string, models.NodeResult) {
	t.Helper()

	outputs, err := node.Execute(execution(), nil)
	require.NoError(t, err)
	require.Len(t, outputs, 1)

	for port, result := range outputs {
		return port, result
	}

	return "", models.NodeResult{}
}

func slackNode(t *testing.T, url string) *NotifyNode {
	t.Helper()

	node, err := NewNotifyNode("notify", map[string]any{
		keyChannel:    ChannelSlack,
		keyMessage:    "Bem-vinda, {{.trigger_data.body.name}}",
		keyWebhookURL: url,
	}, SMTP{})
	require.NoError(t, err)

	return node
}

type sent struct {
	addr string
	auth smtp.Auth
	from string
	to   []string
	msg  string
}

func emailNode(t *testing.T, mail SMTP, extra map[string]any, deliveries *[]sent, failure error) *NotifyNode {
	t.Helper()

	config := map[string]any{
		keyChannel: ChannelEmail,
		keyMessage: "{{.trigger_data.body.name}} joined today",
		keyTo:      "{{.trigger_data.body.manager}}",
		keySubject: "New hire: {{.trigger_data.body.name}}",
	}
	maps.Copy(config, extra)

	node, err := NewNotifyNode("notify", config, mail)
	require.NoError(t, err)

	node.sendMail = func(addr string, auth smtp.Auth, from string, to []string, msg []byte) error {
		*deliveries = append(*deliveries, sent{addr: addr, auth: auth, from: from, to: to, msg: string(msg)})

		return failure
	}

	return node
}

func TestSlackPostsTheRenderedMessage(t *testing.T) {
	var received map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &received)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	port, result := run(t, slackNode(t, server.URL))

	assert.Equal(t, OutputPortSuccess, port)
	assert.Equal(t, map[string]string{"text": "Bem-vinda, Ana"}, received)
	assert.Equal(t, map[string]any{keyChannel: ChannelSlack, keyMessage: "Bem-vinda, Ana"}, result.Data)
}

func TestSlackFailuresGoToTheErrorPort(t *testing.T) {
	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer rejecting.Close()

	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closed.Close()

	tests := []struct {
		name    string
		url     string
		message string
	}{
		{name: "rejected", url: rejecting.URL, message: "slack answered 403"},
		{name: "unreachable", url: closed.URL, message: "slack request failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			port, result := run(t, slackNode(t, tt.url))

			assert.Equal(t, OutputPortError, port)
			assert.Contains(t, result.Error, tt.message)
			assert.Equal(t, string(models.NodeStatusError), result.Status)
		})
	}
}

func TestEmailSendsThroughTheOperatorSMTPToEveryRecipient(t *testing.T) {
	var deliveries []sent

	mail := mailServer()
	mail.Username = "user"
	mail.Password = "secret"
	node := emailNode(t, mail, nil, &deliveries, nil)

	port, result := run(t, node)

	require.Equal(t, OutputPortSuccess, port)
	require.Len(t, deliveries, 1)
	assert.Equal(t, "smtp.example.com:2525", deliveries[0].addr)
	assert.NotNil(t, deliveries[0].auth)
	assert.Equal(t, "operion@example.com", deliveries[0].from)
	assert.Equal(t, []string{"lead@example.com", "hr@example.com"}, deliveries[0].to)
	assert.Contains(t, deliveries[0].msg, "Subject: New hire: Ana\r\n")
	assert.Contains(t, deliveries[0].msg, "To: lead@example.com, hr@example.com\r\n")
	assert.Contains(t, deliveries[0].msg, "\r\n\r\nAna joined today")
	assert.Equal(t, []string{"lead@example.com", "hr@example.com"}, result.Data[keyTo])
}

func TestEmailWithoutCredentialsSkipsAuth(t *testing.T) {
	var deliveries []sent

	port, _ := run(t, emailNode(t, mailServer(), nil, &deliveries, nil))

	require.Equal(t, OutputPortSuccess, port)
	assert.Nil(t, deliveries[0].auth)
}

func TestEmailWithoutOperatorSMTPGoesToTheErrorPort(t *testing.T) {
	for name, mail := range map[string]SMTP{"no server": {}, "no sender": {Host: "smtp.example.com", Port: 25}} {
		t.Run(name, func(t *testing.T) {
			var deliveries []sent

			port, result := run(t, emailNode(t, mail, nil, &deliveries, nil))

			assert.Equal(t, OutputPortError, port)
			assert.Equal(t, errEmailNotConfigured.Error(), result.Error)
			assert.Empty(t, deliveries)
		})
	}
}

func TestEmailFailuresGoToTheErrorPort(t *testing.T) {
	tests := []struct {
		name    string
		extra   map[string]any
		failure error
		message string
	}{
		{name: "delivery error", failure: errors.New("connection refused"), message: "email delivery failed: connection refused"},
		{name: "header injection", extra: map[string]any{keySubject: "hi\r\nBcc: someone@example.com"}, message: "email headers cannot contain line breaks"},
		{name: "broken recipient template", extra: map[string]any{keyTo: brokenTemplate}, message: "failed to parse template"},
		{name: "broken subject template", extra: map[string]any{keySubject: brokenTemplate}, message: "failed to parse template"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var deliveries []sent

			port, result := run(t, emailNode(t, mailServer(), tt.extra, &deliveries, tt.failure))

			assert.Equal(t, OutputPortError, port)
			assert.Contains(t, result.Error, tt.message)
		})
	}
}

func TestBrokenMessageTemplateGoesToTheErrorPort(t *testing.T) {
	node, err := NewNotifyNode("notify", map[string]any{keyChannel: ChannelSlack, keyMessage: brokenTemplate, keyWebhookURL: slackURL}, SMTP{})
	require.NoError(t, err)

	port, result := run(t, node)

	assert.Equal(t, OutputPortError, port)
	assert.Contains(t, result.Error, "failed to parse template")
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]any
		message string
	}{
		{name: "missing message", config: map[string]any{keyChannel: ChannelSlack, keyWebhookURL: slackURL}, message: "missing required field 'message'"},
		{name: "unknown channel", config: map[string]any{keyChannel: "sms", keyMessage: "hi"}, message: `'channel' must be "slack" or "email"`},
		{name: "slack without url", config: map[string]any{keyChannel: ChannelSlack, keyMessage: "hi"}, message: errWebhookURL.Error()},
		{name: "slack with an environment variable name", config: map[string]any{keyChannel: ChannelSlack, keyMessage: "hi", keyWebhookURL: "SLACK_WEBHOOK_URL"}, message: errWebhookURL.Error()},
		{name: "slack with another scheme", config: map[string]any{keyChannel: ChannelSlack, keyMessage: "hi", keyWebhookURL: "file:///etc/passwd"}, message: errWebhookURL.Error()},
		{name: "email without recipients", config: map[string]any{keyChannel: ChannelEmail, keyMessage: "hi"}, message: "email needs 'to'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNotifyNode("notify", tt.config, mailServer())
			require.ErrorContains(t, err, tt.message)

			require.ErrorContains(t, slackNode(t, slackURL).Validate(tt.config), tt.message)
		})
	}

	require.NoError(t, slackNode(t, slackURL).Validate(map[string]any{keyChannel: ChannelEmail, keyMessage: "hi", keyTo: "c@d"}))
}

func TestNodeDescribesItself(t *testing.T) {
	node := slackNode(t, slackURL)

	assert.Equal(t, "notify", node.ID())
	assert.Equal(t, "notify", node.Type())
	require.Len(t, node.InputPorts(), 1)
	assert.Equal(t, "notify:main", node.InputPorts()[0].ID)
	require.Len(t, node.OutputPorts(), 2)
	assert.Equal(t, OutputPortSuccess, node.OutputPorts()[0].Name)
	assert.Equal(t, OutputPortError, node.OutputPorts()[1].Name)

	factory := NewNotifyNodeFactory(mailServer())
	assert.Equal(t, "notify", factory.ID())
	assert.Equal(t, "Notify", factory.Name())
	assert.NotEmpty(t, factory.Description())
	assert.Equal(t, []string{"channel", "message"}, factory.Schema()["required"])

	created, err := factory.Create(context.Background(), "notify", map[string]any{keyChannel: ChannelEmail, keyMessage: "hi", keyTo: "c@d"})
	require.NoError(t, err)
	assert.Equal(t, "notify", created.ID())

	notifyNode, ok := created.(*NotifyNode)
	require.True(t, ok)
	assert.Equal(t, mailServer(), notifyNode.mail)
}
