package conditional

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
)

func TestConditionalNode_Execute_True(t *testing.T) {
	// Create conditional node
	config := map[string]any{
		"condition": "{{.variables.status}} == \"active\"",
	}

	node, err := NewConditionalNode("test-conditional", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create execution context that should evaluate to true
	ctx := models.ExecutionContext{
		ID:          "test-exec",
		WorkflowID:  "test-workflow",
		NodeResults: make(map[string]models.NodeResult),
		Variables:   map[string]any{"status": "active"},
		Metadata:    make(map[string]any),
	}

	// Execute node
	results, err := node.Execute(ctx, make(map[string]models.NodeResult))
	if err != nil {
		t.Fatalf("Node execution failed: %v", err)
	}

	// Verify true output port was used
	trueResult, ok := results[OutputPortTrue]
	if !ok {
		t.Fatal("Expected true output port to be activated")
	}

	if trueResult.Status != string(models.NodeStatusSuccess) {
		t.Errorf("Expected success status, got: %s", trueResult.Status)
	}

	// Verify false output port was NOT used
	if _, ok := results[OutputPortFalse]; ok {
		t.Error("False output port should not be activated when condition is true")
	}
}

func TestConditionalNode_Execute_False(t *testing.T) {
	// Create conditional node with simple boolean condition
	config := map[string]any{
		"condition": "false",
	}

	node, err := NewConditionalNode("test-conditional", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create execution context
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

	// Verify false output port was used
	falseResult, ok := results[OutputPortFalse]
	if !ok {
		t.Fatal("Expected false output port to be activated")
	}

	if falseResult.Status != string(models.NodeStatusSuccess) {
		t.Errorf("Expected success status, got: %s", falseResult.Status)
	}

	// Verify true output port was NOT used
	if _, ok := results[OutputPortTrue]; ok {
		t.Error("True output port should not be activated when condition is false")
	}
}

func TestConditionalNode_Execute_NumberEvaluation(t *testing.T) {
	// Test various data types in condition evaluation
	tests := []struct {
		name       string
		condition  string
		variables  map[string]any
		expectTrue bool
	}{
		{
			name:       "positive number is true",
			condition:  "{{.variables.count}}",
			variables:  map[string]any{"count": 5},
			expectTrue: true,
		},
		{
			name:       "zero is false",
			condition:  "{{.variables.count}}",
			variables:  map[string]any{"count": 0},
			expectTrue: false,
		},
		{
			name:       "non-empty string is true",
			condition:  "{{.variables.name}}",
			variables:  map[string]any{"name": "test"},
			expectTrue: true,
		},
		{
			name:       "empty string is false",
			condition:  "{{.variables.name}}",
			variables:  map[string]any{"name": ""},
			expectTrue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := map[string]any{"condition": tt.condition}

			node, err := NewConditionalNode("test-conditional", config)
			if err != nil {
				t.Fatalf("Failed to create node: %v", err)
			}

			ctx := models.ExecutionContext{
				ID:          "test-exec",
				WorkflowID:  "test-workflow",
				NodeResults: make(map[string]models.NodeResult),
				Variables:   tt.variables,
				Metadata:    make(map[string]any),
			}

			results, err := node.Execute(ctx, make(map[string]models.NodeResult))
			if err != nil {
				t.Fatalf("Node execution failed: %v", err)
			}

			if tt.expectTrue {
				if _, ok := results[OutputPortTrue]; !ok {
					t.Error("Expected true output port to be activated")
				}
			} else {
				if _, ok := results[OutputPortFalse]; !ok {
					t.Error("Expected false output port to be activated")
				}
			}
		})
	}
}

func TestConditionalNode_Validate(t *testing.T) {
	node := &ConditionalNode{}

	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "missing condition",
			config:  map[string]any{},
			wantErr: true,
		},
		{
			name:    "valid condition",
			config:  map[string]any{"condition": "true"},
			wantErr: false,
		},
		{
			name:    "valid templated condition",
			config:  map[string]any{"condition": "{{.variables.status}} == \"ready\""},
			wantErr: false,
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

func TestConditionalNode_Schema(t *testing.T) {
	node := &ConditionalNode{id: "test-node"}

	inputPorts := node.InputPorts()
	if len(inputPorts) == 0 {
		t.Error("Expected input ports to be defined")
	}

	outputPorts := node.OutputPorts()
	if len(outputPorts) != 3 {
		t.Errorf("Expected 3 output ports, got: %d", len(outputPorts))
	}

	// Verify all expected output ports
	expectedPorts := []string{OutputPortTrue, OutputPortFalse, OutputPortError}

	foundPorts := make(map[string]bool)
	for _, port := range outputPorts {
		foundPorts[port.Name] = true
	}

	for _, port := range expectedPorts {
		if !foundPorts[port] {
			t.Errorf("Expected output port '%s' to be defined", port)
		}
	}
}
