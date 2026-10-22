// Package template provides templating functionality for dynamic workflow configuration.
package template

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/dukex/operion/pkg/models"
)

var ErrSecretNotFound = errors.New("secret not found")

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

	return render(input, enhancedData, executionCtx.Secrets)
}

func parse(input string, secrets map[string]string) (*template.Template, error) {
	tmpl, err := template.
		New("transform").
		Funcs(template.FuncMap{
			"secret": func(name string) (string, error) {
				if value, ok := secrets[name]; ok {
					return value, nil
				}

				return "", fmt.Errorf("%w: %s", ErrSecretNotFound, name)
			},
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
	return render(templateStr, data, nil)
}

func render(templateStr string, data any, secrets map[string]string) (any, error) {
	tmpl, err := parse(templateStr, secrets)
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
