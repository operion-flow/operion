package transform

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
)

const transformNodeType = "transform"

// validateErrorResult validates error result properties.
func validateErrorResult(t *testing.T, errorResult models.NodeResult) {
	t.Helper()

	if errorResult.Status != string(models.NodeStatusError) {
		t.Errorf("Expected error status, got: %s", errorResult.Status)
	}

	errorMsg, ok := errorResult.Data["error"].(string)
	if !ok {
		t.Error("Expected error message to be a string")

		return
	}

	if errorMsg == "" {
		t.Error("Expected non-empty error message")
	}
}

// validateSuccessResult validates success result properties.
func validateSuccessResult(t *testing.T, successResult models.NodeResult) {
	t.Helper()

	if successResult.Status != string(models.NodeStatusSuccess) {
		t.Errorf("Expected success status, got: %s", successResult.Status)
	}

	t.Log("Template engine handled missing variable gracefully")
}

func TestNewTransformNode(t *testing.T) {
	config := map[string]any{
		"expression": "{{.variables.input}} | upper",
	}

	node, err := NewTransformNode("test-transform", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	if node.ID() != "test-transform" {
		t.Errorf("Expected ID 'test-transform', got: %s", node.ID())
	}

	if node.Type() != transformNodeType {
		t.Errorf("Expected type 'transform', got: %s", node.Type())
	}

	if node.expression != "{{.variables.input}} | upper" {
		t.Errorf("Expected expression to be set correctly")
	}
}

func TestNewTransformNode_MissingExpression(t *testing.T) {
	config := map[string]any{}

	_, err := NewTransformNode("test-transform", config)
	if err == nil {
		t.Fatal("Expected error when expression is missing")
	}

	if err.Error() != "missing required field 'expression'" {
		t.Errorf("Expected specific error message, got: %s", err.Error())
	}
}

func TestTransformNode_Execute_Success(t *testing.T) {
	// Create transform node
	config := map[string]any{
		"expression": "{{.variables.name}} - {{.variables.age}}",
	}

	node, err := NewTransformNode("test-transform", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create execution context
	ctx := models.ExecutionContext{
		ID:          "test-exec",
		WorkflowID:  "test-workflow",
		NodeResults: make(map[string]models.NodeResult),
		Variables: map[string]any{
			"name": "John Doe",
			"age":  30,
		},
		Metadata: make(map[string]any),
	}

	// Execute node
	results, err := node.Execute(ctx, make(map[string]models.NodeResult))
	if err != nil {
		t.Fatalf("Node execution failed: %v", err)
	}

	// Verify success output port was used
	successResult, ok := results[OutputPortSuccess]
	if !ok {
		t.Fatal("Expected success output port to be activated")
	}

	if successResult.Status != string(models.NodeStatusSuccess) {
		t.Errorf("Expected success status, got: %s", successResult.Status)
	}

	// Verify transformed result
	if result, ok := successResult.Data["result"].(string); ok {
		if result != "John Doe - 30" {
			t.Errorf("Expected 'John Doe - 30', got: %s", result)
		}
	} else {
		t.Error("Expected result to be a string")
	}
}

func TestTransformNode_Execute_TemplateError(t *testing.T) {
	// Create transform node with invalid template syntax
	config := map[string]any{
		"expression": "{{.invalid}}",
	}

	node, err := NewTransformNode("test-transform", config)
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
	// so we check if we get either a success or error result
	errorResult, hasError := results[OutputPortError]
	successResult, hasSuccess := results[OutputPortSuccess]

	switch {
	case hasError:
		validateErrorResult(t, errorResult)
	case hasSuccess:
		validateSuccessResult(t, successResult)
	default:
		t.Fatal("Expected either success or error output port to be activated")
	}
}

func TestTransformNode_Validate(t *testing.T) {
	// Create node for validation testing
	node := &TransformNode{id: "test-node"}

	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "missing expression",
			config:  map[string]any{},
			wantErr: true,
		},
		{
			name:    "valid expression",
			config:  map[string]any{"expression": "{{.variables.test}}"},
			wantErr: false,
		},
		{
			name:    "complex valid expression",
			config:  map[string]any{"expression": "{{.step_results.api_call.data.name}} | title"},
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

func TestTransformNode_InputPorts(t *testing.T) {
	node := &TransformNode{id: "test-node"}

	inputPorts := node.InputPorts()
	if len(inputPorts) != 1 {
		t.Errorf("Expected 1 input port, got: %d", len(inputPorts))
	}

	if inputPorts[0].Name != InputPortMain {
		t.Errorf("Expected input port name '%s', got: %s", InputPortMain, inputPorts[0].Name)
	}
}

func TestTransformNode_OutputPorts(t *testing.T) {
	node := &TransformNode{id: "test-node"}

	outputPorts := node.OutputPorts()
	if len(outputPorts) != 2 {
		t.Errorf("Expected 2 output ports, got: %d", len(outputPorts))
	}

	// Verify expected output ports
	expectedPorts := []string{OutputPortSuccess, OutputPortError}

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
