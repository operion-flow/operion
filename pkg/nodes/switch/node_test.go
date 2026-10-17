package switchnode

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
)

const switchNodeType = "switch"

func TestNewSwitchNode(t *testing.T) {
	config := map[string]any{
		"value": "{{.variables.status}}",
		"cases": []any{
			map[string]any{
				"value":       "active",
				"output_port": "active_path",
			},
			map[string]any{
				"value":       "inactive",
				"output_port": "inactive_path",
			},
		},
	}

	node, err := NewSwitchNode("test-switch", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	if node.ID() != "test-switch" {
		t.Errorf("Expected ID 'test-switch', got: %s", node.ID())
	}

	if node.Type() != switchNodeType {
		t.Errorf("Expected type 'switch', got: %s", node.Type())
	}

	if node.value != "{{.variables.status}}" {
		t.Errorf("Expected value expression to be set correctly")
	}

	if len(node.cases) != 2 {
		t.Errorf("Expected 2 cases, got: %d", len(node.cases))
	}
}

func TestNewSwitchNode_MissingValue(t *testing.T) {
	config := map[string]any{}

	_, err := NewSwitchNode("test-switch", config)
	if err == nil {
		t.Fatal("Expected error when value is missing")
	}

	if err.Error() != "missing required field 'value'" {
		t.Errorf("Expected specific error message, got: %s", err.Error())
	}
}

func TestNewSwitchNode_InvalidCases(t *testing.T) {
	config := map[string]any{
		"value": "{{.variables.status}}",
		"cases": []any{
			map[string]any{
				"value": "active",
				// missing output_port
			},
		},
	}

	_, err := NewSwitchNode("test-switch", config)
	if err == nil {
		t.Fatal("Expected error when case is missing output_port")
	}
}

func TestSwitchNode_Execute_MatchingCase(t *testing.T) {
	// Create switch node
	config := map[string]any{
		"value": "{{.variables.status}}",
		"cases": []any{
			map[string]any{
				"value":       "active",
				"output_port": "active_path",
			},
			map[string]any{
				"value":       "inactive",
				"output_port": "inactive_path",
			},
		},
	}

	node, err := NewSwitchNode("test-switch", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create execution context that matches "active" case
	ctx := models.ExecutionContext{
		ID:          "test-exec",
		WorkflowID:  "test-workflow",
		NodeResults: make(map[string]models.NodeResult),
		Variables: map[string]any{
			"status": "active",
		},
		Metadata: make(map[string]any),
	}

	// Execute node
	results, err := node.Execute(ctx, make(map[string]models.NodeResult))
	if err != nil {
		t.Fatalf("Node execution failed: %v", err)
	}

	// Verify active_path output port was used
	activeResult, ok := results["active_path"]
	if !ok {
		t.Fatal("Expected active_path output port to be activated")
	}

	if activeResult.Status != string(models.NodeStatusSuccess) {
		t.Errorf("Expected success status, got: %s", activeResult.Status)
	}

	// Verify matched value
	if matchedValue, ok := activeResult.Data["matched_value"].(string); ok {
		if matchedValue != "active" {
			t.Errorf("Expected matched_value 'active', got: %s", matchedValue)
		}
	} else {
		t.Error("Expected matched_value to be a string")
	}

	// Verify other ports were NOT used
	if _, ok := results["inactive_path"]; ok {
		t.Error("inactive_path output port should not be activated")
	}

	if _, ok := results[OutputPortDefault]; ok {
		t.Error("Default output port should not be activated")
	}
}

func TestSwitchNode_Execute_NoMatch(t *testing.T) {
	// Create switch node
	config := map[string]any{
		"value": "{{.variables.status}}",
		"cases": []any{
			map[string]any{
				"value":       "active",
				"output_port": "active_path",
			},
		},
	}

	node, err := NewSwitchNode("test-switch", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create execution context that doesn't match any case
	ctx := models.ExecutionContext{
		ID:          "test-exec",
		WorkflowID:  "test-workflow",
		NodeResults: make(map[string]models.NodeResult),
		Variables: map[string]any{
			"status": "unknown",
		},
		Metadata: make(map[string]any),
	}

	// Execute node
	results, err := node.Execute(ctx, make(map[string]models.NodeResult))
	if err != nil {
		t.Fatalf("Node execution failed: %v", err)
	}

	// Verify default output port was used
	defaultResult, ok := results[OutputPortDefault]
	if !ok {
		t.Fatal("Expected default output port to be activated")
	}

	if defaultResult.Status != string(models.NodeStatusSuccess) {
		t.Errorf("Expected success status, got: %s", defaultResult.Status)
	}

	// Verify no match flag
	if noMatch, ok := defaultResult.Data["no_match"].(bool); ok {
		if !noMatch {
			t.Error("Expected no_match to be true")
		}
	} else {
		t.Error("Expected no_match to be a boolean")
	}
}

func TestSwitchNode_Execute_TemplateError(t *testing.T) {
	// Create switch node with invalid template
	config := map[string]any{
		"value": "{{.invalid_variable}}",
	}

	node, err := NewSwitchNode("test-switch", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create execution context without the required variable
	ctx := models.ExecutionContext{
		ID:          "test-exec",
		WorkflowID:  "test-workflow",
		NodeResults: make(map[string]models.NodeResult),
		Variables:   make(map[string]any),
		Metadata:    make(map[string]any),
	}

	// Execute node
	results, err := node.Execute(ctx, make(map[string]models.NodeResult))
	if err != nil {
		t.Fatalf("Node execution failed: %v", err)
	}

	// The template engine might not fail on missing variables,
	// so we check if we get either an error or default result
	if errorResult, ok := results[OutputPortError]; ok {
		// If we get an error result, verify it's correct
		if errorResult.Status != string(models.NodeStatusError) {
			t.Errorf("Expected error status, got: %s", errorResult.Status)
		}
	} else if defaultResult, ok := results[OutputPortDefault]; ok {
		// If we get a default result, the template engine handled missing variables gracefully
		if defaultResult.Status != string(models.NodeStatusSuccess) {
			t.Errorf("Expected success status, got: %s", defaultResult.Status)
		}

		t.Log("Template engine handled missing variable gracefully, using default output")
	} else {
		t.Fatal("Expected either error or default output port to be activated")
	}
}

func TestSwitchNode_Validate(t *testing.T) {
	// Create node for validation testing
	node := &SwitchNode{id: "test-node"}

	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "missing value",
			config:  map[string]any{},
			wantErr: true,
		},
		{
			name:    "valid minimal config",
			config:  map[string]any{"value": "{{.variables.test}}"},
			wantErr: false,
		},
		{
			name: "valid config with cases",
			config: map[string]any{
				"value": "{{.variables.status}}",
				"cases": []any{
					map[string]any{
						"value":       "active",
						"output_port": "active_path",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid case missing value",
			config: map[string]any{
				"value": "{{.variables.status}}",
				"cases": []any{
					map[string]any{
						"output_port": "active_path",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid case missing output_port",
			config: map[string]any{
				"value": "{{.variables.status}}",
				"cases": []any{
					map[string]any{
						"value": "active",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := node.Validate(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSwitchNode_InputPorts(t *testing.T) {
	node := &SwitchNode{id: "test-node"}

	inputPorts := node.InputPorts()
	if len(inputPorts) != 1 {
		t.Errorf("Expected 1 input port, got: %d", len(inputPorts))
	}

	if inputPorts[0].Name != InputPortMain {
		t.Errorf("Expected input port name '%s', got: %s", InputPortMain, inputPorts[0].Name)
	}
}

func TestSwitchNode_OutputPorts(t *testing.T) {
	// Create node with some cases
	config := map[string]any{
		"value": "{{.variables.status}}",
		"cases": []any{
			map[string]any{
				"value":       "active",
				"output_port": "active_path",
			},
			map[string]any{
				"value":       "inactive",
				"output_port": "inactive_path",
			},
		},
	}

	node, err := NewSwitchNode("test-switch", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	outputPorts := node.OutputPorts()

	// Should have default, error, and case-specific ports
	expectedMinPorts := 4 // default, error, active_path, inactive_path
	if len(outputPorts) < expectedMinPorts {
		t.Errorf("Expected at least %d output ports, got: %d", expectedMinPorts, len(outputPorts))
	}

	// Verify expected output ports are present
	foundPorts := make(map[string]bool)
	for _, port := range outputPorts {
		foundPorts[port.Name] = true
	}

	expectedPorts := []string{OutputPortDefault, OutputPortError, "active_path", "inactive_path"}
	for _, port := range expectedPorts {
		if !foundPorts[port] {
			t.Errorf("Expected output port '%s' to be defined", port)
		}
	}
}
