// Package switch provides multi-way switch node implementation for workflow graph execution.
package switchnode

import (
	"errors"
	"fmt"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
)

const (
	OutputPortDefault = "default"
	OutputPortError   = "error"
	InputPortMain     = "main"
)

// SwitchNode implements the Node interface for multi-way branching
// Routes execution to different output ports based on a value.
type SwitchNode struct {
	id    string
	value string            // Expression to evaluate
	cases map[string]string // case_value -> output_port mapping
}

// SwitchCase represents a single case in the switch statement.
type SwitchCase struct {
	Value      string `json:"value"`
	OutputPort string `json:"output_port"`
}

// NewSwitchNode creates a new switch node.
func NewSwitchNode(id string, config map[string]any) (*SwitchNode, error) {
	// Parse value expression (required)
	value, ok := config["value"].(string)
	if !ok {
		return nil, errors.New("missing required field 'value'")
	}

	// Parse cases
	cases := make(map[string]string)

	if casesConfig, ok := config["cases"].([]any); ok {
		for i, caseAny := range casesConfig {
			caseMap, ok := caseAny.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("case %d must be an object", i)
			}

			caseValue, ok := caseMap["value"].(string)
			if !ok {
				return nil, fmt.Errorf("case %d missing 'value'", i)
			}

			outputPort, ok := caseMap["output_port"].(string)
			if !ok {
				return nil, fmt.Errorf("case %d missing 'output_port'", i)
			}

			cases[caseValue] = outputPort
		}
	}

	return &SwitchNode{
		id:    id,
		value: value,
		cases: cases,
	}, nil
}

// ID returns the node ID.
func (n *SwitchNode) ID() string {
	return n.id
}

// Type returns the node type.
func (n *SwitchNode) Type() string {
	return "switch"
}

// Execute evaluates the value and routes to the matching output port.
func (n *SwitchNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	// Render the value expression using the execution context
	result, err := template.RenderWithContext(n.value, &ctx)
	if err != nil {
		return n.createErrorResult(fmt.Sprintf("value evaluation failed: %v", err)), nil
	}

	// Convert result to string for comparison
	valueStr := fmt.Sprintf("%v", result)

	// Find matching case
	if outputPort, exists := n.cases[valueStr]; exists {
		return map[string]models.NodeResult{
			outputPort: {
				NodeID: n.id,
				Data: map[string]any{
					"matched_value": valueStr,
					"output_port":   outputPort,
				},
				Status: string(models.NodeStatusSuccess),
			},
		}, nil
	}

	// No match found - use default port
	return map[string]models.NodeResult{
		OutputPortDefault: {
			NodeID: n.id,
			Data: map[string]any{
				"matched_value": valueStr,
				"output_port":   OutputPortDefault,
				"no_match":      true,
			},
			Status: string(models.NodeStatusSuccess),
		},
	}, nil
}

// createErrorResult creates a NodeResult for the error output port.
func (n *SwitchNode) createErrorResult(errorMessage string) map[string]models.NodeResult {
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
func (n *SwitchNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, InputPortMain),
				NodeID:      n.id,
				Name:        InputPortMain,
				Description: "Main input for triggering the switch evaluation",
			},
		},
	}
}

// OutputPorts returns the output ports for the node.
func (n *SwitchNode) OutputPorts() []models.OutputPort {
	ports := []models.OutputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortDefault),
				NodeID:      n.id,
				Name:        OutputPortDefault,
				Description: "Default execution path when no cases match",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"matched_value": map[string]any{"type": "string"},
						"output_port":   map[string]any{"type": "string"},
						"no_match":      map[string]any{"type": "boolean"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortError),
				NodeID:      n.id,
				Name:        OutputPortError,
				Description: "Error information when switch evaluation fails",
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

	// Add dynamic output ports for each case
	for _, outputPort := range n.cases {
		if outputPort != OutputPortDefault && outputPort != OutputPortError {
			ports = append(ports, models.OutputPort{
				Port: models.Port{
					ID:          models.MakePortID(n.id, outputPort),
					NodeID:      n.id,
					Name:        outputPort,
					Description: fmt.Sprintf("Execution path for case leading to '%s'", outputPort),
					Schema: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"matched_value": map[string]any{"type": "string"},
							"output_port":   map[string]any{"type": "string"},
						},
					},
				},
			})
		}
	}

	return ports
}

// Validate validates the node configuration.
func (n *SwitchNode) Validate(config map[string]any) error {
	if _, ok := config["value"]; !ok {
		return errors.New("missing required field 'value'")
	}

	// Validate cases if provided
	if casesConfig, ok := config["cases"].([]any); ok {
		for i, caseAny := range casesConfig {
			caseMap, ok := caseAny.(map[string]any)
			if !ok {
				return fmt.Errorf("case %d must be an object", i)
			}

			if _, ok := caseMap["value"].(string); !ok {
				return fmt.Errorf("case %d missing 'value'", i)
			}

			if _, ok := caseMap["output_port"].(string); !ok {
				return fmt.Errorf("case %d missing 'output_port'", i)
			}
		}
	}

	return nil
}
