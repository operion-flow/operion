// Package template provides templating functionality for dynamic workflow configuration.
package template

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/dukex/operion/pkg/models"
)

var (
	ErrEnvNotAllowed = errors.New("environment variable is not in --allow-env")
	ErrEnvNotSet     = errors.New("environment variable in --allow-env is not set")

	allowedEnv atomic.Pointer[map[string]string]
)

// AllowEnv lets templates read the named environment variables with {{env "NAME"}}, using their values at the time of the call.
func AllowEnv(names []string) error {
	values := make(map[string]string, len(names))

	for _, name := range names {
		value, ok := os.LookupEnv(name)
		if !ok {
			return fmt.Errorf("%w: %s", ErrEnvNotSet, name)
		}

		values[name] = value
	}

	allowedEnv.Store(&values)

	return nil
}

func env(name string) (string, error) {
	if values := allowedEnv.Load(); values != nil {
		if value, ok := (*values)[name]; ok {
			return value, nil
		}
	}

	return "", fmt.Errorf("%w: %s", ErrEnvNotAllowed, name)
}

func RenderWithContext(input string, executionCtx *models.ExecutionContext) (any, error) {
	// Flatten node results for easier template access
	flattenedNodeResults := make(map[string]any)
	for nodeID, result := range executionCtx.NodeResults {
		flattenedNodeResults[nodeID] = result.Data
	}

	enhancedData := map[string]any{
		"node_results": flattenedNodeResults,
		"variables":    executionCtx.Variables,
		"trigger_data": executionCtx.TriggerData,
		"metadata":     executionCtx.Metadata,
		"execution": map[string]any{
			"id":          executionCtx.ID,
			"workflow_id": executionCtx.WorkflowID,
		},
	}

	return Render(input, enhancedData)
}

// Parse parses the input string as a template and returns the parsed template.
func Parse(input string) (*template.Template, error) {
	tmpl, err := template.
		New("transform").
		Funcs(template.FuncMap{
			"env": env,
			"now": func() string {
				return time.Now().UTC().Format(time.RFC3339)
			},
			"rand": func(maxPossible int32) int32 {
				if maxPossible <= 0 {
					return 0
				}
				num := make([]byte, 1)
				_, err := rand.Read(num)
				if err != nil {
					return 0
				}

				return int32(num[0]) % maxPossible
			},
		}).Parse(input)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template '%s': %w", input, err)
	}

	return tmpl, nil
}

// Render renders the input string as a template with the provided data.
func Render(templateStr string, data any) (any, error) {
	tmpl, err := Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template '%s': %w", templateStr, err)
	}

	var buf strings.Builder

	err = tmpl.Execute(&buf, data)
	if err != nil {
		return nil, fmt.Errorf("failed to execute template '%s': %w", templateStr, err)
	}

	result := buf.String()

	// Try to parse as JSON if it looks like JSON
	result = strings.TrimSpace(result)
	if (strings.HasPrefix(result, "{") && strings.HasSuffix(result, "}")) ||
		(strings.HasPrefix(result, "[") && strings.HasSuffix(result, "]")) {
		var jsonResult any

		err := json.Unmarshal([]byte(result), &jsonResult)
		if err == nil {
			return jsonResult, nil
		}

		return jsonResult, fmt.Errorf("failed to parse json '%s': %w", templateStr, err)
	}

	// Try to parse as number
	if num, err := strconv.ParseFloat(result, 64); err == nil {
		return num, nil
	}

	// Try to parse as boolean
	if b, err := strconv.ParseBool(result); err == nil {
		return b, nil
	}

	// // Return as string
	return result, nil
}
