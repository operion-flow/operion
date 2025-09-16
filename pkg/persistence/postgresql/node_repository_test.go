package postgresql_test

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestWorkflowForNodes(t *testing.T) *models.Workflow {
	t.Helper()

	sourceID := uuid.New().String()

	return &models.Workflow{
		ID:          uuid.New().String(),
		Name:        "Node Test Workflow",
		Description: "A workflow for testing nodes",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "trigger1",
				Type:       "trigger:scheduler",
				Category:   models.CategoryTypeTrigger,
				Name:       "Daily Schedule",
				Config:     map[string]any{"cron": "0 0 * * *"},
				SourceID:   &sourceID,
				ProviderID: &[]string{"scheduler"}[0],
				EventType:  &[]string{"schedule_due"}[0],
				Enabled:    true,
				PositionX:  100,
				PositionY:  200,
			},
			{
				ID:        "action1",
				Type:      "log",
				Category:  models.CategoryTypeAction,
				Name:      "Log Message",
				Config:    map[string]any{"message": "Hello World", "level": "info"},
				Enabled:   true,
				PositionX: 300,
				PositionY: 400,
			},
		},
		Status: models.WorkflowStatusPublished,
		Owner:  "test-user",
	}
}

func TestNodeRepository_SaveAndGetNode(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowForNodes(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	nodeRepo := p.NodeRepository()

	// Test SaveNode - new node
	newNode := &models.WorkflowNode{
		ID:        "new_node",
		Type:      "transform",
		Category:  models.CategoryTypeAction,
		Name:      "Transform Data",
		Config:    map[string]any{"expression": "$.data", "input": "test"},
		Enabled:   true,
		PositionX: 500,
		PositionY: 600,
	}

	err = nodeRepo.SaveNode(ctx, workflow.ID, newNode)
	require.NoError(t, err)

	// Test GetNodeByWorkflow
	retrieved, err := nodeRepo.GetNodeByWorkflow(ctx, workflow.ID, "new_node")
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, newNode.ID, retrieved.ID)
	assert.Equal(t, newNode.Type, retrieved.Type)
	assert.Equal(t, newNode.Category, retrieved.Category)
	assert.Equal(t, newNode.Name, retrieved.Name)
	assert.Equal(t, newNode.Enabled, retrieved.Enabled)
	assert.Equal(t, newNode.PositionX, retrieved.PositionX)
	assert.Equal(t, newNode.PositionY, retrieved.PositionY)
	assert.Equal(t, newNode.Config["expression"], retrieved.Config["expression"])
	assert.Equal(t, newNode.Config["input"], retrieved.Config["input"])
}

func TestNodeRepository_UpdateNode(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowForNodes(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	nodeRepo := p.NodeRepository()

	// Get existing node
	node, err := nodeRepo.GetNodeByWorkflow(ctx, workflow.ID, "action1")
	require.NoError(t, err)
	require.NotNil(t, node)

	// Update node
	node.Name = "Updated Log Message"
	node.Config["message"] = "Updated Hello World"
	node.Config["level"] = "warn"
	node.PositionX = 350
	node.PositionY = 450

	err = nodeRepo.UpdateNode(ctx, workflow.ID, node)
	require.NoError(t, err)

	// Verify update
	updated, err := nodeRepo.GetNodeByWorkflow(ctx, workflow.ID, "action1")
	require.NoError(t, err)
	require.NotNil(t, updated)

	assert.Equal(t, "Updated Log Message", updated.Name)
	assert.Equal(t, "Updated Hello World", updated.Config["message"])
	assert.Equal(t, "warn", updated.Config["level"])
	assert.Equal(t, 350, updated.PositionX)
	assert.Equal(t, 450, updated.PositionY)
}

func TestNodeRepository_DeleteNode(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowForNodes(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	nodeRepo := p.NodeRepository()

	// Verify node exists
	node, err := nodeRepo.GetNodeByWorkflow(ctx, workflow.ID, "action1")
	require.NoError(t, err)
	require.NotNil(t, node)

	// Delete node
	err = nodeRepo.DeleteNode(ctx, workflow.ID, "action1")
	require.NoError(t, err)

	// Verify node is deleted
	deleted, err := nodeRepo.GetNodeByWorkflow(ctx, workflow.ID, "action1")
	require.Error(t, err)
	assert.Nil(t, deleted)
	assert.ErrorIs(t, err, persistence.ErrNodeNotFound)

	// Test deleting non-existent node
	err = nodeRepo.DeleteNode(ctx, workflow.ID, "non_existent")
	require.Error(t, err)
	assert.ErrorIs(t, err, persistence.ErrNodeNotFound)
}

func TestNodeRepository_GetNodesByWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowForNodes(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	nodeRepo := p.NodeRepository()

	// Get all nodes
	nodes, err := nodeRepo.GetNodesByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)

	assert.Len(t, nodes, 2)

	// Verify nodes
	nodeMap := make(map[string]*models.WorkflowNode)
	for _, node := range nodes {
		nodeMap[node.ID] = node
	}

	trigger := nodeMap["trigger1"]
	require.NotNil(t, trigger)
	assert.Equal(t, models.CategoryTypeTrigger, trigger.Category)
	assert.Equal(t, "trigger:scheduler", trigger.Type)
	assert.Equal(t, "scheduler", *trigger.ProviderID)
	assert.Equal(t, "schedule_due", *trigger.EventType)

	action := nodeMap["action1"]
	require.NotNil(t, action)
	assert.Equal(t, models.CategoryTypeAction, action.Category)
	assert.Equal(t, "log", action.Type)
	assert.Equal(t, "Hello World", action.Config["message"])
}

func TestNodeRepository_FindTriggerNodesBySourceEventAndProvider(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	// Create multiple workflows with different triggers
	sourceID1 := uuid.New().String()
	sourceID2 := uuid.New().String()

	workflow1 := &models.Workflow{
		ID:          uuid.New().String(),
		Name:        "Workflow 1",
		Description: "First workflow",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "trigger1",
				Type:       "trigger:scheduler",
				Category:   models.CategoryTypeTrigger,
				Name:       "Schedule Trigger",
				Config:     map[string]any{"cron": "0 0 * * *"},
				SourceID:   &sourceID1,
				ProviderID: &[]string{"scheduler"}[0],
				EventType:  &[]string{"schedule_due"}[0],
				Enabled:    true,
			},
		},
		Status: models.WorkflowStatusPublished,
		Owner:  "test-user",
	}

	workflow2 := &models.Workflow{
		ID:          uuid.New().String(),
		Name:        "Workflow 2",
		Description: "Second workflow",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "trigger2",
				Type:       "trigger:webhook",
				Category:   models.CategoryTypeTrigger,
				Name:       "Webhook Trigger",
				Config:     map[string]any{"path": "/webhook"},
				SourceID:   &sourceID2,
				ProviderID: &[]string{"webhook"}[0],
				EventType:  &[]string{"webhook_received"}[0],
				Enabled:    true,
			},
		},
		Status: models.WorkflowStatusPublished,
		Owner:  "test-user",
	}

	workflow3 := &models.Workflow{
		ID:          uuid.New().String(),
		Name:        "Workflow 3",
		Description: "Third workflow with same trigger as first",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "trigger3",
				Type:       "trigger:scheduler",
				Category:   models.CategoryTypeTrigger,
				Name:       "Another Schedule Trigger",
				Config:     map[string]any{"cron": "0 12 * * *"},
				SourceID:   &sourceID1, // Same source as workflow1
				ProviderID: &[]string{"scheduler"}[0],
				EventType:  &[]string{"schedule_due"}[0],
				Enabled:    true,
			},
		},
		Status: models.WorkflowStatusDraft, // Different status
		Owner:  "test-user",
	}

	// Save all workflows
	err := p.WorkflowRepository().Save(ctx, workflow1)
	require.NoError(t, err)
	err = p.WorkflowRepository().Save(ctx, workflow2)
	require.NoError(t, err)
	err = p.WorkflowRepository().Save(ctx, workflow3)
	require.NoError(t, err)

	nodeRepo := p.NodeRepository()

	// Test finding triggers by source, event, and provider for active workflows
	matches, err := nodeRepo.FindTriggerNodesBySourceEventAndProvider(ctx, sourceID1, "schedule_due", "scheduler", models.WorkflowStatusPublished)
	require.NoError(t, err)

	assert.Len(t, matches, 1) // Should only find workflow1 (workflow3 is inactive)
	assert.Equal(t, workflow1.ID, matches[0].WorkflowID)
	assert.Equal(t, "trigger1", matches[0].TriggerNode.ID)

	// Test finding webhook triggers
	matches, err = nodeRepo.FindTriggerNodesBySourceEventAndProvider(ctx, sourceID2, "webhook_received", "webhook", models.WorkflowStatusPublished)
	require.NoError(t, err)

	assert.Len(t, matches, 1)
	assert.Equal(t, workflow2.ID, matches[0].WorkflowID)
	assert.Equal(t, "trigger2", matches[0].TriggerNode.ID)

	// Test finding triggers with different status
	matches, err = nodeRepo.FindTriggerNodesBySourceEventAndProvider(ctx, sourceID1, "schedule_due", "scheduler", models.WorkflowStatusDraft)
	require.NoError(t, err)

	assert.Len(t, matches, 1) // Should find workflow3
	assert.Equal(t, workflow3.ID, matches[0].WorkflowID)
	assert.Equal(t, "trigger3", matches[0].TriggerNode.ID)

	// Test finding non-existent triggers
	matches, err = nodeRepo.FindTriggerNodesBySourceEventAndProvider(ctx, "non-existent", "schedule_due", "scheduler", models.WorkflowStatusPublished)
	require.NoError(t, err)

	assert.Len(t, matches, 0)
}

func TestNodeRepository_ErrorCases(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	nodeRepo := p.NodeRepository()
	nonExistentWorkflowID := uuid.New().String()

	// Test getting node from non-existent workflow
	node, err := nodeRepo.GetNodeByWorkflow(ctx, nonExistentWorkflowID, "some_node")
	require.Error(t, err)
	assert.Nil(t, node)
	assert.ErrorIs(t, err, persistence.ErrNodeNotFound)

	// Test getting nodes from non-existent workflow
	nodes, err := nodeRepo.GetNodesByWorkflow(ctx, nonExistentWorkflowID)
	require.NoError(t, err) // Should not error, just return empty slice
	assert.Len(t, nodes, 0)

	// Test saving node to non-existent workflow
	newNode := &models.WorkflowNode{
		ID:       "test_node",
		Type:     "log",
		Category: models.CategoryTypeAction,
		Name:     "Test Node",
		Config:   map[string]any{"message": "test"},
		Enabled:  true,
	}

	err = nodeRepo.SaveNode(ctx, nonExistentWorkflowID, newNode)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "violates foreign key constraint")
}

func TestNodeRepository_DeleteNodeWithConnections(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowForNodes(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	nodeRepo := p.NodeRepository()
	connRepo := p.ConnectionRepository()

	// Create connections involving the nodes
	connections := []*models.Connection{
		{
			ID:         "conn-1",
			SourcePort: "trigger1:success",
			TargetPort: "action1:input",
		},
		{
			ID:         "conn-2",
			SourcePort: "action1:success",
			TargetPort: "trigger1:input", // This should also be deleted
		},
	}

	for _, conn := range connections {
		err = connRepo.SaveConnection(ctx, workflow.ID, conn)
		require.NoError(t, err)
	}

	// Verify connections exist before deletion
	allConnections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Len(t, allConnections, 2)

	// Delete node with connections
	err = nodeRepo.DeleteNodeWithConnections(ctx, workflow.ID, "trigger1")
	require.NoError(t, err)

	// Verify node is deleted
	_, err = nodeRepo.GetNodeByWorkflow(ctx, workflow.ID, "trigger1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify all connections involving the deleted node are removed
	remainingConnections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Len(t, remainingConnections, 0)

	// Verify other nodes are still present
	remainingNode, err := nodeRepo.GetNodeByWorkflow(ctx, workflow.ID, "action1")
	require.NoError(t, err)
	assert.Equal(t, "action1", remainingNode.ID)
}

func TestNodeRepository_DeleteNodeWithConnections_NodeNotFound(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowForNodes(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	nodeRepo := p.NodeRepository()

	// Try to delete non-existent node
	err = nodeRepo.DeleteNodeWithConnections(ctx, workflow.ID, "nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, persistence.ErrNodeNotFound)
}

func TestNodeRepository_DeleteNodeWithConnections_WorkflowNotFound(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	nodeRepo := p.NodeRepository()
	nonExistentWorkflowID := uuid.New().String()

	// Try to delete node from non-existent workflow
	err := nodeRepo.DeleteNodeWithConnections(ctx, nonExistentWorkflowID, "some-node")
	require.Error(t, err)
	assert.ErrorIs(t, err, persistence.ErrNodeNotFound)
}
