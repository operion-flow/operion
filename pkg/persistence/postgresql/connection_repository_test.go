package postgresql_test

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestWorkflowWithConnections(t *testing.T) *models.Workflow {
	t.Helper()

	sourceID := uuid.New().String()

	return &models.Workflow{
		ID:          uuid.New().String(),
		Name:        "Connection Test Workflow",
		Description: "A workflow for testing connections",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "trigger1",
				Type:       "trigger:webhook",
				Category:   models.CategoryTypeTrigger,
				Name:       "Webhook Trigger",
				Config:     map[string]any{"path": "/webhook"},
				SourceID:   &sourceID,
				ProviderID: &[]string{"webhook"}[0],
				EventType:  &[]string{"webhook_received"}[0],
				Enabled:    true,
			},
			{
				ID:       "transform1",
				Type:     "transform",
				Category: models.CategoryTypeAction,
				Name:     "Transform Data",
				Config:   map[string]any{"expression": "$.data"},
				Enabled:  true,
			},
			{
				ID:       "log1",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Name:     "Log Result",
				Config:   map[string]any{"message": "Result: {{.result}}"},
				Enabled:  true,
			},
			{
				ID:       "error_handler",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Name:     "Error Handler",
				Config:   map[string]any{"message": "Error: {{.error}}", "level": "error"},
				Enabled:  true,
			},
		},
		Connections: []*models.Connection{
			{
				ID:         "conn1",
				SourcePort: "trigger1:output",
				TargetPort: "transform1:input",
			},
			{
				ID:         "conn2",
				SourcePort: "transform1:success",
				TargetPort: "log1:input",
			},
			{
				ID:         "conn3",
				SourcePort: "transform1:error",
				TargetPort: "error_handler:input",
			},
		},
		Status: models.WorkflowStatusPublished,
		Owner:  "test-user",
	}
}

func TestConnectionRepository_SaveAndGetConnection(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowWithConnections(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	connRepo := p.ConnectionRepository()

	// Test SaveConnection - new connection
	newConnection := &models.Connection{
		ID:         "new_conn",
		SourcePort: "log1:success",
		TargetPort: "error_handler:notify",
	}

	err = connRepo.SaveConnection(ctx, workflow.ID, newConnection)
	require.NoError(t, err)

	// Test GetConnectionsByWorkflow
	connections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)

	assert.Len(t, connections, 4) // 3 original + 1 new

	// Find our new connection
	var foundConnection *models.Connection

	for _, conn := range connections {
		if conn.ID == "new_conn" {
			foundConnection = conn

			break
		}
	}

	require.NotNil(t, foundConnection)
	assert.Equal(t, newConnection.ID, foundConnection.ID)
	assert.Equal(t, newConnection.SourcePort, foundConnection.SourcePort)
	assert.Equal(t, newConnection.TargetPort, foundConnection.TargetPort)
}

func TestConnectionRepository_GetConnectionsBySourceNode(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowWithConnections(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	connRepo := p.ConnectionRepository()

	// Test GetConnectionsBySourceNode - filter by source node
	connections, err := connRepo.GetConnectionsBySourceNode(ctx, workflow.ID, "transform1")
	require.NoError(t, err)

	assert.Len(t, connections, 2) // transform1 has 2 outgoing connections

	// Verify connections
	portMap := make(map[string]string)
	for _, conn := range connections {
		portMap[conn.SourcePort] = conn.TargetPort
	}

	assert.Equal(t, "log1:input", portMap["transform1:success"])
	assert.Equal(t, "error_handler:input", portMap["transform1:error"])

	// Test with source node that has only one connection
	connections, err = connRepo.GetConnectionsBySourceNode(ctx, workflow.ID, "trigger1")
	require.NoError(t, err)

	assert.Len(t, connections, 1)
	assert.Equal(t, "trigger1:output", connections[0].SourcePort)
	assert.Equal(t, "transform1:input", connections[0].TargetPort)

	// Test with source node that has no connections
	connections, err = connRepo.GetConnectionsBySourceNode(ctx, workflow.ID, "log1")
	require.NoError(t, err)

	assert.Len(t, connections, 0)
}

func TestConnectionRepository_GetConnectionsByTargetNode(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowWithConnections(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	connRepo := p.ConnectionRepository()

	// Test GetConnectionsByTargetNode
	connections, err := connRepo.GetConnectionsByTargetNode(ctx, workflow.ID, "error_handler")
	require.NoError(t, err)

	assert.Len(t, connections, 1)
	assert.Equal(t, "transform1:error", connections[0].SourcePort)
	assert.Equal(t, "error_handler:input", connections[0].TargetPort)

	// Test with target node that receives multiple connections
	// First add another connection to log1
	newConnection := &models.Connection{
		ID:         "conn4",
		SourcePort: "trigger1:notify",
		TargetPort: "log1:notify",
	}

	err = connRepo.SaveConnection(ctx, workflow.ID, newConnection)
	require.NoError(t, err)

	connections, err = connRepo.GetConnectionsByTargetNode(ctx, workflow.ID, "log1")
	require.NoError(t, err)

	assert.Len(t, connections, 2)

	// Test with target node that has no incoming connections
	connections, err = connRepo.GetConnectionsByTargetNode(ctx, workflow.ID, "transform1")
	require.NoError(t, err)

	assert.Len(t, connections, 1) // Only trigger1 -> transform1
	assert.Equal(t, "trigger1:output", connections[0].SourcePort)
}

func TestConnectionRepository_UpdateConnection(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowWithConnections(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	connRepo := p.ConnectionRepository()

	// Get existing connection
	connections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)

	var targetConnection *models.Connection

	for _, conn := range connections {
		if conn.ID == "conn1" {
			targetConnection = conn

			break
		}
	}

	require.NotNil(t, targetConnection)

	// Update connection
	targetConnection.TargetPort = "log1:direct"

	err = connRepo.UpdateConnection(ctx, workflow.ID, targetConnection)
	require.NoError(t, err)

	// Verify update
	updated, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)

	var updatedConnection *models.Connection

	for _, conn := range updated {
		if conn.ID == "conn1" {
			updatedConnection = conn

			break
		}
	}

	require.NotNil(t, updatedConnection)

	assert.Equal(t, "trigger1:output", updatedConnection.SourcePort)
	assert.Equal(t, "log1:direct", updatedConnection.TargetPort)
}

func TestConnectionRepository_DeleteConnection(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowWithConnections(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	connRepo := p.ConnectionRepository()

	// Verify connection exists
	connections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)

	initialCount := len(connections)

	// Delete connection
	err = connRepo.DeleteConnection(ctx, workflow.ID, "conn2")
	require.NoError(t, err)

	// Verify connection is deleted
	connections, err = connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)

	assert.Len(t, connections, initialCount-1)

	// Verify the specific connection is gone
	for _, conn := range connections {
		assert.NotEqual(t, "conn2", conn.ID)
	}

	// Test deleting non-existent connection
	err = connRepo.DeleteConnection(ctx, workflow.ID, "non_existent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection not found")
}

func TestConnectionRepository_GetConnectionsByWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowWithConnections(t)

	// Save workflow first
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	connRepo := p.ConnectionRepository()

	// Test GetConnectionsByWorkflow (alias method)
	connections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)

	assert.Len(t, connections, 3)

	// Verify all original connections are present
	connectionMap := make(map[string]*models.Connection)
	for _, conn := range connections {
		connectionMap[conn.ID] = conn
	}

	assert.Contains(t, connectionMap, "conn1")
	assert.Contains(t, connectionMap, "conn2")
	assert.Contains(t, connectionMap, "conn3")
}

func TestConnectionRepository_PortParsing(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	workflow := createTestWorkflowWithConnections(t)

	// Save workflow first (this should create nodes)
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	connRepo := p.ConnectionRepository()

	// Test connection with complex port names
	complexConnection := &models.Connection{
		ID:         "complex_conn",
		SourcePort: "node_with_underscores:output_port_name",
		TargetPort: "another-node-with-dashes:input-port-name",
	}

	err = connRepo.SaveConnection(ctx, workflow.ID, complexConnection)
	require.NoError(t, err)

	// Retrieve and verify
	connections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)

	var foundConnection *models.Connection

	for _, conn := range connections {
		if conn.ID == "complex_conn" {
			foundConnection = conn

			break
		}
	}

	require.NotNil(t, foundConnection)
	assert.Equal(t, "node_with_underscores:output_port_name", foundConnection.SourcePort)
	assert.Equal(t, "another-node-with-dashes:input-port-name", foundConnection.TargetPort)
}

func TestConnectionRepository_ErrorCases(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	connRepo := p.ConnectionRepository()
	nonExistentWorkflowID := uuid.New().String()

	// Test getting connections from non-existent workflow
	connections, err := connRepo.GetConnectionsByWorkflow(ctx, nonExistentWorkflowID)
	require.NoError(t, err) // Should not error, just return empty slice
	assert.Len(t, connections, 0)

	// Test saving connection with invalid port format
	invalidConnection := &models.Connection{
		ID:         "invalid_conn",
		SourcePort: "invalid_port_format", // Missing colon
		TargetPort: "target:input",
	}

	err = connRepo.SaveConnection(ctx, nonExistentWorkflowID, invalidConnection)
	require.Error(t, err)
	assert.True(t, persistence.IsInvalidPortFormat(err), "Should return invalid port format error")

	// Test saving connection with invalid target port format
	invalidConnection2 := &models.Connection{
		ID:         "invalid_conn2",
		SourcePort: "source:output",
		TargetPort: "invalid_target_format", // Missing colon
	}

	err = connRepo.SaveConnection(ctx, nonExistentWorkflowID, invalidConnection2)
	require.Error(t, err)
	assert.True(t, persistence.IsInvalidPortFormat(err), "Should return invalid port format error")
}
