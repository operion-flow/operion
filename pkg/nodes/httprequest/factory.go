// Package httprequest provides HTTP request node factory for the registry system.
package httprequest

import (
	"context"

	"github.com/dukex/operion/pkg/protocol"
)

// HTTPRequestNodeFactory creates HTTPRequestNode instances.
type HTTPRequestNodeFactory struct{}

// NewHTTPRequestNodeFactory creates a new HTTP request node factory.
func NewHTTPRequestNodeFactory() protocol.NodeFactory {
	return &HTTPRequestNodeFactory{}
}

// Create creates a new HTTPRequestNode instance.
func (f *HTTPRequestNodeFactory) Create(ctx context.Context, id string, config map[string]any) (protocol.Node, error) {
	return NewHTTPRequestNode(id, config)
}

// ID returns the factory ID.
func (f *HTTPRequestNodeFactory) ID() string {
	return "httprequest"
}

// Name returns the factory name.
func (f *HTTPRequestNodeFactory) Name() string {
	return "HTTP Request"
}

// Description returns the factory description.
func (f *HTTPRequestNodeFactory) Description() string {
	return "Performs HTTP requests with retry logic and multiple output ports for success/error handling"
}

// Schema returns the JSON schema for HTTP request node configuration.
func (f *HTTPRequestNodeFactory) Schema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"url": map[string]any{
				"type":        "string",
				"description": "HTTP URL to request. Supports templating with {{.node_results.prev_node.result}}",
				"examples": []string{
					"https://api.example.com/users",
					"{{.node_results.get_user_id.user_url}}",
					"https://{{.variables.api_host}}/webhook/{{.trigger_data.webhook.id}}",
				},
			},
			"method": map[string]any{
				"type":        "string",
				"description": "HTTP method",
				"default":     "GET",
				"enum":        []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"},
			},
			"headers": map[string]any{
				"type":        "object",
				"description": "HTTP headers. Values support templating",
				"examples": []map[string]any{
					{"Authorization": "Bearer {{.variables.api_token}}"},
					{"Content-Type": "application/json", "User-Agent": "Operion/1.0"},
				},
			},
			"body": map[string]any{
				"type":        "string",
				"description": "Request body. Supports templating for dynamic content",
				"examples": []string{
					`{"name": "{{.node_results.transform.user_name}}", "email": "{{.trigger_data.webhook.email}}"}`,
					`{{.node_results.previous_step.json_data}}`,
				},
			},
			"timeout": map[string]any{
				"type":        "number",
				"description": "Request timeout in seconds",
				"default":     30,
				"minimum":     1,
				"maximum":     300,
			},
		},
		"required": []string{"url"},
		"examples": []map[string]any{
			{
				"url":    "https://api.github.com/user",
				"method": "GET",
				"headers": map[string]string{
					"Authorization": "Bearer {{.variables.github_token}}",
					"Accept":        "application/vnd.github.v3+json",
				},
			},
			{
				"url":     "{{.node_results.api_discovery.webhook_url}}",
				"method":  "POST",
				"headers": map[string]string{"Content-Type": "application/json"},
				"body":    `{"status": "completed", "result": {{.node_results.data_processing.output}}}`,
			},
		},
	}
}
