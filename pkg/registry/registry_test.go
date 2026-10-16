package registry

import (
	"context"
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock node for testing.
type mockNode struct {
	id     string
	config map[string]any
}

func (m *mockNode) ID() string {
	return m.id
}

func (m *mockNode) Type() string {
	return "mock-node"
}

func (m *mockNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	return map[string]models.NodeResult{
		"output": {
			NodeID: m.id,
			Data: map[string]any{
				"config": m.config,
				"inputs": inputs,
				"result": "mock execution completed",
			},
			Status: "success",
		},
	}, nil
}

func (m *mockNode) InputPorts() []models.InputPort {
	return []models.InputPort{{
		Port: models.Port{
			ID:     m.id + ":input",
			NodeID: m.id,
			Name:   "input",
		},
	}}
}

func (m *mockNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{{
		Port: models.Port{
			ID:     m.id + ":output",
			NodeID: m.id,
			Name:   "output",
		},
	}}
}

func (m *mockNode) Validate(config map[string]any) error {
	return nil
}

// Mock node factory for testing.
type mockNodeFactory struct {
	nodeType string
}

func (f *mockNodeFactory) ID() string {
	return f.nodeType
}

func (f *mockNodeFactory) Name() string {
	return "Mock Node"
}

func (f *mockNodeFactory) Description() string {
	return "This is a mock node for testing purposes."
}

func (f *mockNodeFactory) Create(_ context.Context, nodeID string, config map[string]any) (protocol.Node, error) {
	return &mockNode{
		id:     nodeID,
		config: config,
	}, nil
}

func (f *mockNodeFactory) Schema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"test_field": map[string]any{
				"type":        "string",
				"description": "Test field for mock node",
			},
		},
	}
}

// Mock provider for testing.
func TestNewRegistry(t *testing.T) {
	registry := NewRegistry()

	assert.NotNil(t, registry)
	assert.Empty(t, registry.nodeFactories)
}

func TestRegistry_RegisterAndCreateNode(t *testing.T) {
	registry := NewRegistry()

	// Register mock node factory
	nodeFactory := &mockNodeFactory{nodeType: "test-node"}
	registry.RegisterNode(nodeFactory)

	// Verify registration
	assert.Len(t, registry.nodeFactories, 1)
	assert.Contains(t, registry.nodeFactories, "test-node")

	// Create node instance
	config := map[string]any{
		"param1": "value1",
		"param2": 42,
	}

	node, err := registry.CreateNode(context.Background(), "test-node", "node-123", config)
	require.NoError(t, err)
	assert.NotNil(t, node)

	// Verify node can be executed
	execContext := models.ExecutionContext{ID: "exec-123"}
	inputs := map[string]models.NodeResult{}
	result, err := node.Execute(execContext, inputs)
	require.NoError(t, err)

	assert.Contains(t, result, "output")
	output := result["output"]
	assert.Equal(t, "node-123", output.NodeID)
	assert.Equal(t, "success", output.Status)
	assert.Equal(t, config, output.Data["config"])
	assert.Equal(t, inputs, output.Data["inputs"])
	assert.Equal(t, "mock execution completed", output.Data["result"])
}

func TestRegistry_CreateNode_NotRegistered(t *testing.T) {
	registry := NewRegistry()

	// Try to create node that's not registered
	node, err := registry.CreateNode(context.Background(), "non-existent-node", "node-123", map[string]any{})

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "not registered")
}
