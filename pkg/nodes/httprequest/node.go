// Package httprequest provides HTTP request node implementation for workflow graph execution.
package httprequest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
)

const (
	OutputPortSuccess = "success"
	OutputPortError   = "error"
	InputPortMain     = "main"
)

// HTTPRequestNode implements the Node interface for HTTP requests with multiple output ports.
type HTTPRequestNode struct {
	id     string
	config HTTPRequestConfig
}

// HTTPRequestConfig defines the configuration for HTTP request nodes.
type HTTPRequestConfig struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body,omitempty"`
	Timeout int               `json:"timeout"`
	Retries RetryConfig       `json:"retries"`
}

// RetryConfig defines retry behavior for HTTP requests.
type RetryConfig struct {
	Attempts int `json:"attempts"`
	Delay    int `json:"delay"`
}

// NewHTTPRequestNode creates a new HTTP request node.
func NewHTTPRequestNode(id string, config map[string]any) (*HTTPRequestNode, error) {
	// Parse configuration
	httpConfig := HTTPRequestConfig{
		Method:  "GET",
		Headers: make(map[string]string),
		Timeout: 30,
		Retries: RetryConfig{Attempts: 1, Delay: 0},
	}

	// Parse URL (required)
	if url, ok := config["url"].(string); ok {
		httpConfig.URL = url
	} else {
		return nil, errors.New("missing required field 'url'")
	}

	// Parse optional fields
	if method, ok := config["method"].(string); ok {
		httpConfig.Method = strings.ToUpper(method)
	}

	if headers, ok := config["headers"].(map[string]any); ok {
		for k, v := range headers {
			if strVal, ok := v.(string); ok {
				httpConfig.Headers[k] = strVal
			}
		}
	}

	if body, ok := config["body"].(string); ok {
		httpConfig.Body = body
	}

	if timeout, ok := config["timeout"].(float64); ok {
		httpConfig.Timeout = int(timeout)
	}

	// Parse retries
	if retries, ok := config["retries"].(map[string]any); ok {
		if attempts, ok := retries["attempts"].(float64); ok {
			httpConfig.Retries.Attempts = int(attempts)
		}

		if delay, ok := retries["delay"].(float64); ok {
			httpConfig.Retries.Delay = int(delay)
		}
	}

	return &HTTPRequestNode{
		id:     id,
		config: httpConfig,
	}, nil
}

// ID returns the node ID.
func (n *HTTPRequestNode) ID() string {
	return n.id
}

// Type returns the node type.
func (n *HTTPRequestNode) Type() string {
	return "httprequest"
}

// Execute performs the HTTP request and returns results on appropriate output ports.
func (n *HTTPRequestNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	results := make(map[string]models.NodeResult)

	// Render templates in configuration using execution context
	renderedURL, err := template.RenderWithContext(n.config.URL, &ctx)
	if err != nil {
		return n.createErrorResult(fmt.Sprintf("failed to render URL template: %v", err)), nil
	}

	urlStr, ok := renderedURL.(string)
	if !ok {
		return n.createErrorResult("URL template must render to string"), nil
	}

	// Render body if present
	var renderedBody string

	if n.config.Body != "" {
		renderedBodyAny, err := template.RenderWithContext(n.config.Body, &ctx)
		if err != nil {
			return n.createErrorResult(fmt.Sprintf("failed to render body template: %v", err)), nil
		}

		renderedBody, _ = renderedBodyAny.(string)
	}

	// Render headers
	renderedHeaders := make(map[string]string)

	for key, value := range n.config.Headers {
		renderedValue, err := template.RenderWithContext(value, &ctx)
		if err != nil {
			renderedHeaders[key] = value // Use original value if template fails
		} else if strVal, ok := renderedValue.(string); ok {
			renderedHeaders[key] = strVal
		} else {
			renderedHeaders[key] = value
		}
	}

	// Perform HTTP request with retry logic
	var lastErr error

	for attempt := 1; attempt <= n.config.Retries.Attempts; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(n.config.Retries.Delay) * time.Millisecond)
		}

		result, err := n.performRequest(urlStr, renderedBody, renderedHeaders)
		if err == nil {
			// Success - return result on success port
			results[OutputPortSuccess] = models.NodeResult{
				NodeID: n.id,
				Data:   result,
				Status: string(models.NodeStatusSuccess),
			}

			return results, nil
		}

		lastErr = err

		// Don't retry on client errors (4xx), only on server errors (5xx) or network errors
		httpErr := &HTTPError{}
		if errors.As(err, &httpErr) {
			break
		}
	}

	// All attempts failed - return error on error port
	return n.createErrorResult(fmt.Sprintf("HTTP request failed after %d attempts: %v", n.config.Retries.Attempts, lastErr)), nil
}

// HTTPError represents an HTTP error with status code.
type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}

// performRequest executes a single HTTP request.
func (n *HTTPRequestNode) performRequest(url, body string, headers map[string]string) (map[string]any, error) {
	var reqBody io.Reader
	if body != "" {
		reqBody = strings.NewReader(body)
	}

	req, err := http.NewRequestWithContext(context.TODO(), n.config.Method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// Set default Content-Type if not specified and body is present
	if body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: time.Duration(n.config.Timeout) * time.Second,
	}

	// Perform request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			// Log error but don't fail the request - body close errors are non-critical
			_ = err
		}
	}()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Check for HTTP errors
	if resp.StatusCode >= 400 {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
		}
	}

	// Parse response
	result := map[string]any{
		"status_code": resp.StatusCode,
		"headers":     resp.Header,
		"body":        string(respBody),
	}

	// Try to parse JSON response
	var jsonBody any
	if err := json.Unmarshal(respBody, &jsonBody); err == nil {
		result["json"] = jsonBody
	}

	return result, nil
}

// createErrorResult creates a NodeResult for the error output port.
func (n *HTTPRequestNode) createErrorResult(errorMessage string) map[string]models.NodeResult {
	return map[string]models.NodeResult{
		OutputPortError: {
			NodeID: n.id,
			Data: map[string]any{
				"error":   errorMessage,
				"success": false,
			},
			Status: string(models.NodeStatusError),
		},
	}
}

// InputPorts returns the input ports for the node.
func (n *HTTPRequestNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, InputPortMain),
				NodeID:      n.id,
				Name:        InputPortMain,
				Description: "Main input for triggering the HTTP request",
			},
		},
	}
}

// OutputPorts returns the output ports for the node.
func (n *HTTPRequestNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortSuccess),
				NodeID:      n.id,
				Name:        OutputPortSuccess,
				Description: "Successful HTTP response data",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"status_code": map[string]any{"type": "number"},
						"headers":     map[string]any{"type": "object"},
						"body":        map[string]any{"type": "string"},
						"json":        map[string]any{"type": "object"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortError),
				NodeID:      n.id,
				Name:        OutputPortError,
				Description: "Error information when HTTP request fails",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"error":   map[string]any{"type": "string"},
						"success": map[string]any{"type": "boolean"},
					},
				},
			},
		},
	}
}

// Validate validates the node configuration.
func (n *HTTPRequestNode) Validate(config map[string]any) error {
	if _, ok := config["url"]; !ok {
		return errors.New("missing required field 'url'")
	}

	// Validate method if provided
	if method, ok := config["method"].(string); ok {
		validMethods := map[string]bool{
			"GET": true, "POST": true, "PUT": true, "DELETE": true,
			"PATCH": true, "HEAD": true, "OPTIONS": true,
		}
		if !validMethods[strings.ToUpper(method)] {
			return fmt.Errorf("invalid HTTP method: %s", method)
		}
	}

	// Validate timeout if provided
	if timeout, ok := config["timeout"].(float64); ok {
		if timeout < 1 || timeout > 300 {
			return errors.New("timeout must be between 1 and 300 seconds")
		}
	}

	// Validate retries if provided
	if retries, ok := config["retries"].(map[string]any); ok {
		if err := validateRetryConfig(retries); err != nil {
			return err
		}
	}

	return nil
}

// validateRetryConfig validates retry configuration parameters.
func validateRetryConfig(retries map[string]any) error {
	if attempts, ok := retries["attempts"].(float64); ok {
		if attempts < 1 || attempts > 10 {
			return errors.New("retry attempts must be between 1 and 10")
		}
	}

	if delay, ok := retries["delay"].(float64); ok {
		if delay < 0 || delay > 30000 {
			return errors.New("retry delay must be between 0 and 30000 milliseconds")
		}
	}

	return nil
}
