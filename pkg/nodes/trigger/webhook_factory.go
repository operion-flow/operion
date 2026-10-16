package trigger

import (
	"context"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
)

// WebhookTriggerNodeFactory creates WebhookTriggerNode instances.
type WebhookTriggerNodeFactory struct{}

// NewWebhookTriggerNodeFactory creates a new webhook trigger node factory.
func NewWebhookTriggerNodeFactory() protocol.NodeFactory {
	return &WebhookTriggerNodeFactory{}
}

// Create creates a new WebhookTriggerNode instance.
func (f *WebhookTriggerNodeFactory) Create(ctx context.Context, id string, config map[string]any) (protocol.Node, error) {
	return NewWebhookTriggerNode(id, config)
}

// ID returns the factory ID.
func (f *WebhookTriggerNodeFactory) ID() string {
	return models.NodeTypeTriggerWebhook
}

// Name returns the factory name.
func (f *WebhookTriggerNodeFactory) Name() string {
	return "Webhook Trigger"
}

// Description returns the factory description.
func (f *WebhookTriggerNodeFactory) Description() string {
	return "Receives webhook events from external sources and starts workflow execution"
}

// Schema returns the JSON schema for webhook trigger node configuration.
func (f *WebhookTriggerNodeFactory) Schema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"webhook_path": map[string]any{
				"type":        "string",
				"description": "The webhook endpoint path that will receive HTTP requests",
				"examples": []string{
					"/webhook/orders",
					"/webhook/github-push",
					"/webhook/user-signup",
				},
			},
			"method": map[string]any{
				"type":        "string",
				"description": "HTTP method allowed for the webhook",
				"default":     "POST",
				"enum":        []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
			},
			"headers": map[string]any{
				"type":        "object",
				"description": "Expected HTTP headers for validation (optional)",
				"examples": []map[string]any{
					{"Authorization": "Bearer secret-token"},
					{"X-GitHub-Event": "push", "Content-Type": "application/json"},
				},
			},
		},
		schemaRequired: []string{"webhook_path"},
		"examples": []map[string]any{
			{
				"webhook_path": "/webhook/orders",
				"method":       "POST",
				"headers": map[string]string{
					"Content-Type": "application/json",
				},
			},
			{
				"webhook_path": "/webhook/github",
				"method":       "POST",
				"headers": map[string]string{
					"X-GitHub-Event": "push",
				},
			},
		},
	}
}
