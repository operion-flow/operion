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
					"/orders",
					"/github-push",
					"/user-signup",
				},
			},
		},
		schemaRequired: []string{"webhook_path"},
		"examples": []map[string]any{
			{"webhook_path": "/orders"},
		},
	}
}
