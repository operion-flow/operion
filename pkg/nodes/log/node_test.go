package log

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
)

func TestLogNode_Execute_Info(t *testing.T) {
	// Create log node
	config := map[string]any{
		"message": "Processing user: {{.variables.user_name}}",
		"level":   "info",
	}

	node, err := NewLogNode("test-log", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create execution context
	ctx := models.ExecutionContext{
		ID:          "test-exec",
		WorkflowID:  "test-workflow",
		NodeResults: make(map[string]models.NodeResult),
		Variables:   map[string]any{"user_name": "john_doe"},
		Metadata:    make(map[string]any),
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

	// Verify logged message
	if message, ok := successResult.Data["message"].(string); ok {
		if message != "Processing user: john_doe" {
			t.Errorf("Expected 'Processing user: john_doe', got: %s", message)
		}
	} else {
		t.Error("Expected message field in result data")
	}

	// Verify level
	if level, ok := successResult.Data["level"].(string); ok {
		if level != "info" {
			t.Errorf("Expected level 'info', got: %s", level)
		}
	} else {
		t.Error("Expected level field in result data")
	}
}

func TestLogNode_Execute_Error_Level(t *testing.T) {
	// Create log node with error level
	config := map[string]any{
		"message": "Critical error occurred",
		"level":   "error",
	}

	node, err := NewLogNode("test-log-error", config)
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

	// Verify success output (logging an error message is still successful execution)
	successResult, ok := results[OutputPortSuccess]
	if !ok {
		t.Fatal("Expected success output port to be activated")
	}

	// Verify level is error
	if level, ok := successResult.Data["level"].(string); ok {
		if level != "error" {
			t.Errorf("Expected level 'error', got: %s", level)
		}
	}
}

func TestLogNode_Execute_DefaultLevel(t *testing.T) {
	// Create log node without specifying level (should default to info)
	config := map[string]any{
		"message": "Default level message",
	}

	node, err := NewLogNode("test-log-default", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Execute and verify default level is "info"
	ctx := models.ExecutionContext{
		ID:          "test-exec",
		WorkflowID:  "test-workflow",
		NodeResults: make(map[string]models.NodeResult),
		Variables:   make(map[string]any),
		Metadata:    make(map[string]any),
	}

	results, err := node.Execute(ctx, make(map[string]models.NodeResult))
	if err != nil {
		t.Fatalf("Node execution failed: %v", err)
	}

	successResult := results[OutputPortSuccess]
	if level, ok := successResult.Data["level"].(string); ok {
		if level != "info" {
			t.Errorf("Expected default level 'info', got: %s", level)
		}
	}
}

func TestLogNode_Execute_TemplateError(t *testing.T) {
	// Create log node with invalid template syntax (missing closing brace)
	config := map[string]any{
		"message": "Invalid template: {{.variables.user",
		"level":   "info",
	}

	node, err := NewLogNode("test-log-template-error", config)
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

	// Verify error output port was used
	errorResult, ok := results[OutputPortError]
	if !ok {
		t.Fatal("Expected error output port to be activated for template error")
	}

	if errorResult.Status != string(models.NodeStatusError) {
		t.Errorf("Expected error status, got: %s", errorResult.Status)
	}
}

func TestLogNode_Validate(t *testing.T) {
	node := &LogNode{}

	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "missing message",
			config:  map[string]any{},
			wantErr: true,
		},
		{
			name:    "valid config with message only",
			config:  map[string]any{"message": "test message"},
			wantErr: false,
		},
		{
			name:    "valid config with level",
			config:  map[string]any{"message": "test", "level": "debug"},
			wantErr: false,
		},
		{
			name:    "invalid level",
			config:  map[string]any{"message": "test", "level": "invalid"},
			wantErr: true,
		},
		{
			name:    "valid levels",
			config:  map[string]any{"message": "test", "level": "warn"},
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

func TestLogNode_Schema(t *testing.T) {
	node := &LogNode{id: "test-node"}

	inputPorts := node.InputPorts()
	if len(inputPorts) == 0 {
		t.Error("Expected input ports to be defined")
	}

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

func TestLogNodeFactory(t *testing.T) {
	const expectedLogID = "log"

	factory := NewLogNodeFactory()

	// Test factory metadata
	if factory.ID() != expectedLogID {
		t.Errorf("Expected ID '%s', got: %s", expectedLogID, factory.ID())
	}

	if factory.Name() != "Log" {
		t.Errorf("Expected name 'Log', got: %s", factory.Name())
	}

	// Test schema
	schema := factory.Schema()
	if schema == nil {
		t.Fatal("Expected schema to be defined")
	}

	// Verify required fields
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatal("Expected properties in schema")
	}

	if _, ok := properties["message"]; !ok {
		t.Error("Expected 'message' property in schema")
	}

	if _, ok := properties["level"]; !ok {
		t.Error("Expected 'level' property in schema")
	}
}
