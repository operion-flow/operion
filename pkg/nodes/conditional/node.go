// Package conditional provides conditional branching node implementation for workflow graph execution.
package conditional

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
)

const (
	OutputPortTrue  = "true"
	OutputPortFalse = "false"
	OutputPortError = "error"
	InputPortMain   = "main"
)

// ConditionalNode implements the Node interface for conditional branching
// This is a key control flow node that enables different execution paths.
type ConditionalNode struct {
	id        string
	condition string
}

// NewConditionalNode creates a new conditional branching node.
func NewConditionalNode(id string, config map[string]any) (*ConditionalNode, error) {
	// Parse condition (required)
	condition, ok := config["condition"].(string)
	if !ok {
		return nil, errors.New("missing required field 'condition'")
	}

	return &ConditionalNode{
		id:        id,
		condition: condition,
	}, nil
}

// ID returns the node ID.
func (n *ConditionalNode) ID() string {
	return n.id
}

// Type returns the node type.
func (n *ConditionalNode) Type() string {
	return "conditional"
}

// Execute evaluates the condition and routes to true/false output ports.
func (n *ConditionalNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	// Render the condition expression using the execution context
	result, err := template.RenderWithContext(n.condition, &ctx)
	if err != nil {
		return n.createErrorResult(fmt.Sprintf("condition evaluation failed: %v", err)), nil
	}

	// Convert result to boolean
	isTrue := n.evaluateCondition(result)

	// Route to appropriate output port
	if isTrue {
		return map[string]models.NodeResult{
			OutputPortTrue: {
				NodeID: n.id,
				Data: map[string]any{
					"condition_result": true,
					"evaluated_value":  result,
				},
				Status: string(models.NodeStatusSuccess),
			},
		}, nil
	} else {
		return map[string]models.NodeResult{
			OutputPortFalse: {
				NodeID: n.id,
				Data: map[string]any{
					"condition_result": false,
					"evaluated_value":  result,
				},
				Status: string(models.NodeStatusSuccess),
			},
		}, nil
	}
}

// evaluateCondition converts various types to boolean.
func (n *ConditionalNode) evaluateCondition(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		// Handle string boolean values
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		// Non-empty strings are truthy
		return v != ""
	case int, int64, int32:
		return v != 0
	case float64, float32:
		return v != 0.0
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	case nil:
		return false
	default:
		// Unknown types default to false
		return false
	}
}

// createErrorResult creates a NodeResult for the error output port.
func (n *ConditionalNode) createErrorResult(errorMessage string) map[string]models.NodeResult {
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
func (n *ConditionalNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, InputPortMain),
				NodeID:      n.id,
				Name:        InputPortMain,
				Description: "Main input for triggering the condition evaluation",
			},
		},
	}
}

// OutputPorts returns the output ports for the node.
func (n *ConditionalNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortTrue),
				NodeID:      n.id,
				Name:        OutputPortTrue,
				Description: "Execution path when condition evaluates to true",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"condition_result": map[string]any{"type": "boolean"},
						"evaluated_value":  map[string]any{"type": "any"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortFalse),
				NodeID:      n.id,
				Name:        OutputPortFalse,
				Description: "Execution path when condition evaluates to false",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"condition_result": map[string]any{"type": "boolean"},
						"evaluated_value":  map[string]any{"type": "any"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortError),
				NodeID:      n.id,
				Name:        OutputPortError,
				Description: "Error information when condition evaluation fails",
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
func (n *ConditionalNode) Validate(config map[string]any) error {
	if _, ok := config["condition"]; !ok {
		return errors.New("missing required field 'condition'")
	}

	return nil
}
