package registry

import (
	"context"
	"errors"
	"testing"
)

func TestRegisterDefaultNodes(t *testing.T) {
	// Create a new registry
	registry := NewRegistry()

	// Register default nodes
	registry.RegisterDefaultNodes()

	// Verify all expected node types are registered
	expectedNodes := []string{
		"httprequest",
		"transform",
		"log",
		"conditional",
		"switch",
		"trigger:webhook",
		"trigger:scheduler",
		"trigger:kafka",
	}

	if len(registry.nodeFactories) != len(expectedNodes) {
		t.Errorf("Expected %d nodes, got %d", len(expectedNodes), len(registry.nodeFactories))
	}

	for _, expectedType := range expectedNodes {
		if _, found := registry.nodeFactories[expectedType]; !found {
			t.Errorf("Expected node type '%s' not found in registry", expectedType)
		}
	}
}

func TestCreateNode_HTTPRequest(t *testing.T) {
	// Create registry and register nodes
	registry := NewRegistry()
	registry.RegisterDefaultNodes()

	// Test creating an HTTP request node
	config := map[string]any{
		"url":    "https://api.example.com/test",
		"method": "GET",
	}

	node, err := registry.CreateNode(context.Background(), "httprequest", "test-node-1", config)
	if err != nil {
		t.Fatalf("Failed to create HTTP request node: %v", err)
	}

	if node.ID() != "test-node-1" {
		t.Errorf("Expected node ID 'test-node-1', got: %s", node.ID())
	}

	if node.Type() != "httprequest" {
		t.Errorf("Expected node type 'httprequest', got: %s", node.Type())
	}
}

func TestCreateNode_Transform(t *testing.T) {
	// Create registry and register nodes
	registry := NewRegistry()
	registry.RegisterDefaultNodes()

	// Test creating a transform node
	config := map[string]any{
		"expression": `{"result": "{{.variables.input}}"}`,
	}

	node, err := registry.CreateNode(context.Background(), "transform", "transform-node-1", config)
	if err != nil {
		t.Fatalf("Failed to create transform node: %v", err)
	}

	if node.ID() != "transform-node-1" {
		t.Errorf("Expected node ID 'transform-node-1', got: %s", node.ID())
	}

	if node.Type() != "transform" {
		t.Errorf("Expected node type 'transform', got: %s", node.Type())
	}
}

func TestCreateNode_Conditional(t *testing.T) {
	// Create registry and register nodes
	registry := NewRegistry()
	registry.RegisterDefaultNodes()

	// Test creating a conditional node
	config := map[string]any{
		"condition": `{{.variables.status}} == "active"`,
	}

	node, err := registry.CreateNode(context.Background(), "conditional", "cond-node-1", config)
	if err != nil {
		t.Fatalf("Failed to create conditional node: %v", err)
	}

	if node.ID() != "cond-node-1" {
		t.Errorf("Expected node ID 'cond-node-1', got: %s", node.ID())
	}

	if node.Type() != "conditional" {
		t.Errorf("Expected node type 'conditional', got: %s", node.Type())
	}
}

func TestCreateNode_Log(t *testing.T) {
	// Create registry and register nodes
	registry := NewRegistry()
	registry.RegisterDefaultNodes()

	// Test creating a log node
	config := map[string]any{
		"message": "Test log message: {{.variables.test_var}}",
		"level":   "info",
	}

	node, err := registry.CreateNode(context.Background(), "log", "log-node-1", config)
	if err != nil {
		t.Fatalf("Failed to create log node: %v", err)
	}

	if node.ID() != "log-node-1" {
		t.Errorf("Expected node ID 'log-node-1', got: %s", node.ID())
	}

	if node.Type() != "log" {
		t.Errorf("Expected node type 'log', got: %s", node.Type())
	}
}

func TestCreateNode_UnknownType(t *testing.T) {
	// Create registry and register nodes
	registry := NewRegistry()
	registry.RegisterDefaultNodes()

	// Test creating a node with unknown type
	config := map[string]any{}

	_, err := registry.CreateNode(context.Background(), "unknown_type", "test-node", config)
	if err == nil {
		t.Fatal("Expected error when creating node with unknown type")
	}

	if !errors.Is(err, ErrNodeNotRegistered) {
		t.Errorf("Expected ErrNodeNotRegistered, got: %v", err)
	}
}
