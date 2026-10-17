// Package transform provides data transformation node implementation for workflow graph execution.
package transform

import (
	"errors"
	"fmt"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
)

const (
	OutputPortSuccess = "success"
	OutputPortError   = "error"
	InputPortMain     = "main"
)

// TransformNode implements the Node interface for data transformation.
type TransformNode struct {
	id         string
	expression string
}

// NewTransformNode creates a new data transformation node.
func NewTransformNode(id string, config map[string]any) (*TransformNode, error) {
	// Parse expression (required)
	expression, ok := config["expression"].(string)
	if !ok {
		return nil, errors.New("missing required field 'expression'")
	}

	return &TransformNode{
		id:         id,
		expression: expression,
	}, nil
}

// ID returns the node ID.
func (n *TransformNode) ID() string {
	return n.id
}

// Type returns the node type.
func (n *TransformNode) Type() string {
	return "transform"
}

// Execute performs data transformation using Go templates.
func (n *TransformNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	// Render the transformation expression using the execution context
	result, err := template.RenderWithContext(n.expression, &ctx)
	if err != nil {
		return n.createErrorResult(fmt.Sprintf("transformation failed: %v", err)), nil
	}

	// Success - return result on success port
	return map[string]models.NodeResult{
		OutputPortSuccess: {
			NodeID: n.id,
			Data: map[string]any{
				"result": result,
			},
			Status: string(models.NodeStatusSuccess),
		},
	}, nil
}

// createErrorResult creates a NodeResult for the error output port.
func (n *TransformNode) createErrorResult(errorMessage string) map[string]models.NodeResult {
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
func (n *TransformNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, InputPortMain),
				NodeID:      n.id,
				Name:        InputPortMain,
				Description: "Main input for triggering the transformation",
			},
		},
	}
}

// OutputPorts returns the output ports for the node.
func (n *TransformNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortSuccess),
				NodeID:      n.id,
				Name:        OutputPortSuccess,
				Description: "Transformed data result",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"result": map[string]any{"type": "any", "description": "The transformed result"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortError),
				NodeID:      n.id,
				Name:        OutputPortError,
				Description: "Error information when transformation fails",
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
func (n *TransformNode) Validate(config map[string]any) error {
	if _, ok := config["expression"]; !ok {
		return errors.New("missing required field 'expression'")
	}

	return nil
}
