package webhook

import (
	"log/slog"

	"github.com/dukex/operion/pkg/protocol"
)

const (
	webhookProviderID = "webhook"
)

// WebhookProviderFactory creates instances of WebhookProvider.
type WebhookProviderFactory struct{}

// NewWebhookProviderFactory creates a new factory instance.
func NewWebhookProviderFactory() *WebhookProviderFactory {
	return &WebhookProviderFactory{}
}

// Create instantiates a new centralized WebhookProvider orchestrator.
func (f *WebhookProviderFactory) Create(config map[string]any, logger *slog.Logger) (protocol.Provider, error) {
	// Create single orchestrator instance (port configuration handled during Initialize)
	return &WebhookProvider{
		config: config,
		logger: logger.With("module", "centralized_webhook"),
	}, nil
}

// ID returns the unique identifier for this source provider type.
func (f *WebhookProviderFactory) ID() string {
	return webhookProviderID
}

// Name returns a human-readable name for this source provider.
func (f *WebhookProviderFactory) Name() string {
	return "Centralized Webhook"
}

// Description returns a detailed description of what this source provider does.
func (f *WebhookProviderFactory) Description() string {
	return "A centralized webhook orchestrator that manages HTTP webhook endpoints with external ID-based security. Receives HTTP POST requests and converts them to source events for workflow triggering. Supports optional JSON schema validation and automatic source registration from workflow triggers."
}

// Schema returns a JSON Schema that describes the orchestrator configuration.
func (f *WebhookProviderFactory) Schema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"port": map[string]any{
				"type":        "integer",
				"description": "Port number for the webhook HTTP server (default: 8085)",
				"minimum":     1,
				"maximum":     65535,
				"examples":    []int{8080, 8085, 9000},
				"default":     8085,
			},
			"max_request_size": map[string]any{
				"type":        "integer",
				"description": "Maximum request body size in bytes (default: 1048576 = 1MB)",
				"minimum":     1024,
				"maximum":     10485760,
				"examples":    []int{1048576, 5242880, 10485760},
				"default":     1048576,
			},
			"timeout": map[string]any{
				"type":        "object",
				"description": "HTTP server timeout configuration",
				"properties": map[string]any{
					"read": map[string]any{
						"type":        "string",
						"description": "Read timeout duration (default: 30s)",
						"examples":    []string{"30s", "1m", "2m"},
						"default":     "30s",
					},
					"write": map[string]any{
						"type":        "string",
						"description": "Write timeout duration (default: 30s)",
						"examples":    []string{"30s", "1m", "2m"},
						"default":     "30s",
					},
					"idle": map[string]any{
						"type":        "string",
						"description": "Idle timeout duration (default: 60s)",
						"examples":    []string{"60s", "2m", "5m"},
						"default":     "60s",
					},
				},
				"additionalProperties": false,
			},
		},
		"required":             []string{},
		"additionalProperties": false,
		"description":          "Centralized webhook orchestrator configuration. Individual webhook sources are created automatically from workflow triggers with 'trigger_id':'webhook'.",
		"examples": []map[string]any{
			{
				"port":             8085,
				"max_request_size": 1048576,
			},
			{
				"port": 9000,
				"timeout": map[string]any{
					"read":  "45s",
					"write": "45s",
					"idle":  "2m",
				},
			},
		},
	}
}

// EventTypes returns a list of event types that this source provider can emit.
func (f *WebhookProviderFactory) EventTypes() []string {
	return []string{"WebhookReceived"}
}

// Ensure interface compliance.
var _ protocol.ProviderFactory = (*WebhookProviderFactory)(nil)
