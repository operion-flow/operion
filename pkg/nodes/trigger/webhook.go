// Package trigger provides trigger node implementations for workflow graph execution.
package trigger

import (
	"errors"

	"github.com/dukex/operion/pkg/models"
)

const (
	WebhookInputPortExternal = "external"
	WebhookOutputPortSuccess = "success"
	WebhookOutputPortError   = "error"
)

// WebhookTriggerNode implements the Node interface for webhook triggers.
type WebhookTriggerNode struct {
	id     string
	config WebhookTriggerConfig
}

// WebhookTriggerConfig defines the configuration for webhook trigger nodes.
type WebhookTriggerConfig struct {
	WebhookPath string            `json:"webhook_path"`
	Headers     map[string]string `json:"headers"`
	Method      string            `json:"method"`
}

// NewWebhookTriggerNode creates a new webhook trigger node.
func NewWebhookTriggerNode(id string, config map[string]any) (*WebhookTriggerNode, error) {
	// Parse configuration
	webhookConfig := WebhookTriggerConfig{
		Method:  "POST",
		Headers: make(map[string]string),
	}

	// Parse webhook_path (required)
	if webhookPath, ok := config["webhook_path"].(string); ok {
		webhookConfig.WebhookPath = webhookPath
	} else {
		return nil, errors.New("webhook_path is required")
	}

	// Parse method
	if method, ok := config["method"].(string); ok {
		webhookConfig.Method = method
	}

	// Parse headers
	if headers, ok := config["headers"].(map[string]any); ok {
		for k, v := range headers {
			if headerValue, ok := v.(string); ok {
				webhookConfig.Headers[k] = headerValue
			}
		}
	}

	return &WebhookTriggerNode{
		id:     id,
		config: webhookConfig,
	}, nil
}

// ID returns the node ID.
func (n *WebhookTriggerNode) ID() string {
	return n.id
}

// Type returns the node type.
func (n *WebhookTriggerNode) Type() string {
	return models.NodeTypeTriggerWebhook
}

// Execute processes the webhook event data from external input.
func (n *WebhookTriggerNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	results := make(map[string]models.NodeResult)

	// Get external input
	externalInput, exists := inputs[WebhookInputPortExternal]
	if !exists {
		return n.createErrorResult("external input not found"), nil
	}

	// Process webhook data
	webhookData := externalInput.Data

	// Create success result with webhook data
	results[WebhookOutputPortSuccess] = models.NodeResult{
		NodeID: n.id,
		Data: map[string]any{
			"headers": webhookData["headers"],
			"body":    webhookData["body"],
			"method":  webhookData["method"],
			"url":     webhookData["url"],
			"query":   webhookData["query"],
		},
		Status: string(models.NodeStatusSuccess),
	}

	return results, nil
}

// createErrorResult creates an error result for the error output port.
func (n *WebhookTriggerNode) createErrorResult(message string) map[string]models.NodeResult {
	return map[string]models.NodeResult{
		WebhookOutputPortError: {
			NodeID: n.id,
			Data: map[string]any{
				"error":   message,
				"node_id": n.id,
			},
			Status: string(models.NodeStatusError),
			Error:  message,
		},
	}
}

// InputPorts returns the input ports for the webhook trigger node.
func (n *WebhookTriggerNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, WebhookInputPortExternal),
				NodeID:      n.id,
				Name:        WebhookInputPortExternal,
				Description: "External webhook event input",
				Schema: map[string]any{
					"type":        "object",
					"description": "Webhook event data from external source",
					"properties": map[string]any{
						"headers": map[string]any{"type": "object"},
						"body":    map[string]any{"type": "string"},
						"method":  map[string]any{"type": "string"},
						"url":     map[string]any{"type": "string"},
						"query":   map[string]any{"type": "object"},
					},
				},
			},
		},
	}
}

// OutputPorts returns the output ports for the webhook trigger node.
func (n *WebhookTriggerNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, WebhookOutputPortSuccess),
				NodeID:      n.id,
				Name:        WebhookOutputPortSuccess,
				Description: "Successful webhook processing result",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"headers": map[string]any{"type": "object"},
						"body":    map[string]any{"type": "string"},
						"method":  map[string]any{"type": "string"},
						"url":     map[string]any{"type": "string"},
						"query":   map[string]any{"type": "object"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, WebhookOutputPortError),
				NodeID:      n.id,
				Name:        WebhookOutputPortError,
				Description: "Webhook processing error",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"error":   map[string]any{"type": "string"},
						"node_id": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
}

// Validate validates the node configuration.
func (n *WebhookTriggerNode) Validate(config map[string]any) error {
	if webhookPath, ok := config["webhook_path"].(string); !ok || webhookPath == "" {
		return errors.New("webhook_path is required and must be a non-empty string")
	}

	if method, ok := config["method"].(string); ok {
		allowedMethods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
		found := false

		for _, allowed := range allowedMethods {
			if method == allowed {
				found = true

				break
			}
		}

		if !found {
			return errors.New("method must be one of: GET, POST, PUT, DELETE, PATCH")
		}
	}

	return nil
}
