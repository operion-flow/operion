// Package testutil provides test data builders and utilities for testing.
package testutil

import (
	"github.com/dukex/operion/pkg/models"
	"github.com/google/uuid"
)

// CreateTestNode creates a test WorkflowNode with default values that can be overridden.
func CreateTestNode(overrides ...func(*models.WorkflowNode)) *models.WorkflowNode {
	node := &models.WorkflowNode{
		ID:        uuid.New().String(),
		Type:      "log",
		Category:  models.CategoryTypeAction,
		Name:      "Test Node",
		Config:    map[string]any{"message": "test", "level": "info"},
		Enabled:   true,
		PositionX: 100,
		PositionY: 200,
	}

	for _, override := range overrides {
		override(node)
	}

	return node
}

// WithTriggerNode configures the node as a trigger node.
func WithTriggerNode() func(*models.WorkflowNode) {
	return func(n *models.WorkflowNode) {
		n.Type = "trigger:webhook"
		n.Category = models.CategoryTypeTrigger
		sourceID := uuid.New().String()
		providerID := "webhook"
		eventType := "webhook_received"
		n.SourceID = &sourceID
		n.ProviderID = &providerID
		n.EventType = &eventType
		n.Config = map[string]any{
			"path":   "/webhook/test",
			"method": "POST",
		}
	}
}

// WithConfig sets the node configuration.
func WithConfig(config map[string]any) func(*models.WorkflowNode) {
	return func(n *models.WorkflowNode) {
		n.Config = config
	}
}

// WithName sets the node name.
func WithName(name string) func(*models.WorkflowNode) {
	return func(n *models.WorkflowNode) {
		n.Name = name
	}
}

// WithPosition sets the node position.
func WithPosition(x, y int) func(*models.WorkflowNode) {
	return func(n *models.WorkflowNode) {
		n.PositionX = x
		n.PositionY = y
	}
}

// WithEnabled sets the node enabled status.
func WithEnabled(enabled bool) func(*models.WorkflowNode) {
	return func(n *models.WorkflowNode) {
		n.Enabled = enabled
	}
}

// WithType sets the node type.
func WithType(nodeType string) func(*models.WorkflowNode) {
	return func(n *models.WorkflowNode) {
		n.Type = nodeType
	}
}

// WithID sets the node ID.
func WithID(id string) func(*models.WorkflowNode) {
	return func(n *models.WorkflowNode) {
		n.ID = id
	}
}

// CreateTestWorkflow creates a test workflow with some default nodes.
func CreateTestWorkflow() *models.Workflow {
	return &models.Workflow{
		ID:          uuid.New().String(),
		Name:        "Test Workflow",
		Description: "A workflow for testing",
		Status:      models.WorkflowStatusDraft,
		Owner:       "test-user",
		Variables:   map[string]any{"env": "test"},
		Metadata:    map[string]any{"category": "test"},
		Nodes:       []*models.WorkflowNode{},
		Connections: []*models.Connection{},
	}
}

// CreateTestWorkflowWithNodes creates a test workflow with predefined nodes.
func CreateTestWorkflowWithNodes() *models.Workflow {
	workflow := CreateTestWorkflow()

	// Add some test nodes
	triggerNode := CreateTestNode(WithTriggerNode(), WithID("trigger-1"))
	actionNode := CreateTestNode(WithID("action-1"), WithName("Log Action"))

	workflow.Nodes = []*models.WorkflowNode{triggerNode, actionNode}

	// Add a test connection
	workflow.Connections = []*models.Connection{
		{
			ID:         "conn-1",
			SourcePort: "trigger-1:success",
			TargetPort: "action-1:input",
		},
	}

	return workflow
}

// CreateTestConnection creates a test connection between two nodes.
func CreateTestConnection(sourceNodeID, targetNodeID string) *models.Connection {
	return &models.Connection{
		ID:         uuid.New().String(),
		SourcePort: sourceNodeID + ":success",
		TargetPort: targetNodeID + ":input",
	}
}
