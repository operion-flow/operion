package postgresql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepositoryIntegration_CompleteWorkflowLifecycle(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	// Step 1: Create and save workflow
	workflow := createCompleteTestWorkflow(t)

	// Step 2-5: Test workflow operations
	testWorkflowOperations(t, p, ctx, workflow)

	// Step 6-8: Test execution context operations
	execCtx := testExecutionContextOperations(t, p, ctx, workflow)

	// Step 9-10: Test workflow modifications
	testWorkflowModifications(t, p, ctx, workflow)

	// Step 11: Test cleanup operations
	testCleanupOperations(t, p, ctx, workflow, execCtx)
}

func createCompleteTestWorkflow(t *testing.T) *models.Workflow {
	t.Helper()

	sourceID := uuid.New().String()

	return &models.Workflow{
		ID:          uuid.New().String(),
		Name:        "Integration Test Workflow",
		Description: "A complete workflow for testing integration",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "webhook_trigger",
				Type:       "trigger:webhook",
				Category:   models.CategoryTypeTrigger,
				Name:       "Webhook Trigger",
				Config:     map[string]any{"path": "/api/webhook", "method": "POST"},
				SourceID:   &sourceID,
				ProviderID: &[]string{"webhook"}[0],
				EventType:  &[]string{"webhook_received"}[0],
				Enabled:    true,
				PositionX:  100,
				PositionY:  100,
			},
			{
				ID:        "validate_data",
				Type:      "transform",
				Category:  models.CategoryTypeAction,
				Name:      "Validate Data",
				Config:    map[string]any{"expression": "$.payload", "validation": "required"},
				Enabled:   true,
				PositionX: 300,
				PositionY: 100,
			},
			{
				ID:        "api_call",
				Type:      "httprequest",
				Category:  models.CategoryTypeAction,
				Name:      "API Call",
				Config:    map[string]any{"url": "https://api.example.com/process", "method": "POST"},
				Enabled:   true,
				PositionX: 500,
				PositionY: 100,
			},
			{
				ID:        "log_result",
				Type:      "log",
				Category:  models.CategoryTypeAction,
				Name:      "Log Result",
				Config:    map[string]any{"message": "Processing complete: {{.result}}", "level": "info"},
				Enabled:   true,
				PositionX: 700,
				PositionY: 100,
			},
			{
				ID:        "error_handler",
				Type:      "log",
				Category:  models.CategoryTypeAction,
				Name:      "Error Handler",
				Config:    map[string]any{"message": "Error: {{.error}}", "level": "error"},
				Enabled:   true,
				PositionX: 500,
				PositionY: 300,
			},
		},
		Connections: []*models.Connection{
			{
				ID:         "conn1",
				SourcePort: "webhook_trigger:output",
				TargetPort: "validate_data:input",
			},
			{
				ID:         "conn2",
				SourcePort: "validate_data:success",
				TargetPort: "api_call:input",
			},
			{
				ID:         "conn3",
				SourcePort: "api_call:success",
				TargetPort: "log_result:input",
			},
			{
				ID:         "conn4",
				SourcePort: "validate_data:error",
				TargetPort: "error_handler:input",
			},
			{
				ID:         "conn5",
				SourcePort: "api_call:error",
				TargetPort: "error_handler:input",
			},
		},
		Variables: map[string]any{
			"api_timeout": 30,
			"retry_count": 3,
			"debug_mode":  true,
		},
		Metadata: map[string]any{
			"version":     "1.0.0",
			"environment": "test",
			"created_by":  "integration_test",
		},
		Status: models.WorkflowStatusPublished,
		Owner:  "test-user",
	}
}

func testWorkflowOperations(t *testing.T, p *postgresql.Persistence, ctx context.Context, workflow *models.Workflow) {
	t.Helper()

	// Step 1: Save the complete workflow using WorkflowRepository
	workflowRepo := p.WorkflowRepository()
	err := workflowRepo.Save(ctx, workflow)
	require.NoError(t, err)

	// Step 2: Verify the workflow and all its components were saved correctly
	retrievedWorkflow, err := workflowRepo.GetByID(ctx, workflow.ID)
	require.NoError(t, err)
	require.NotNil(t, retrievedWorkflow)

	assert.Equal(t, workflow.ID, retrievedWorkflow.ID)
	assert.Equal(t, workflow.Name, retrievedWorkflow.Name)
	assert.Len(t, retrievedWorkflow.Nodes, 5)
	assert.Len(t, retrievedWorkflow.Connections, 5)

	// Step 3: Use NodeRepository to verify individual nodes
	nodeRepo := p.NodeRepository()

	// Get all nodes through NodeRepository
	nodes, err := nodeRepo.GetNodesByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Len(t, nodes, 5)

	// Get specific trigger node
	triggerNode, err := nodeRepo.GetNodeByWorkflow(ctx, workflow.ID, "webhook_trigger")
	require.NoError(t, err)
	require.NotNil(t, triggerNode)
	assert.Equal(t, models.CategoryTypeTrigger, triggerNode.Category)
	assert.Equal(t, "trigger:webhook", triggerNode.Type)

	// Step 4: Use ConnectionRepository to verify connections
	connRepo := p.ConnectionRepository()

	// Get all connections
	connections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Len(t, connections, 5)

	// Get connections by source node
	triggerConnections, err := connRepo.GetConnectionsBySourceNode(ctx, workflow.ID, "webhook_trigger")
	require.NoError(t, err)
	assert.Len(t, triggerConnections, 1)
	assert.Equal(t, "validate_data:input", triggerConnections[0].TargetPort)

	// Get connections by target node (error_handler receives from 2 sources)
	errorConnections, err := connRepo.GetConnectionsByTargetNode(ctx, workflow.ID, "error_handler")
	require.NoError(t, err)
	assert.Len(t, errorConnections, 2)

	// Step 5: Test trigger node finding functionality
	sourceID := *workflow.Nodes[0].SourceID
	triggerMatches, err := nodeRepo.FindTriggerNodesBySourceEventAndProvider(ctx, sourceID, "webhook_received", "webhook", models.WorkflowStatusPublished)
	require.NoError(t, err)
	assert.Len(t, triggerMatches, 1)
	assert.Equal(t, workflow.ID, triggerMatches[0].WorkflowID)
	assert.Equal(t, "webhook_trigger", triggerMatches[0].TriggerNode.ID)
}

func testExecutionContextOperations(t *testing.T, p *postgresql.Persistence, ctx context.Context, workflow *models.Workflow) *models.ExecutionContext {
	t.Helper()

	// Step 6: Create and manage execution context
	execRepo := p.ExecutionContextRepository()

	execCtx := &models.ExecutionContext{
		ID:         uuid.New().String(),
		WorkflowID: workflow.ID,
		Status:     models.ExecutionStatusRunning,
		NodeResults: map[string]models.NodeResult{
			"webhook_trigger": {
				NodeID: "webhook_trigger",
				Data: map[string]any{
					"payload": map[string]any{"user_id": 123, "action": "create"},
					"headers": map[string]any{"Content-Type": "application/json"},
				},
				Status:    "success",
				Timestamp: time.Now().UTC(),
			},
			"validate_data": {
				NodeID: "validate_data",
				Data: map[string]any{
					"validation_result": "passed",
					"extracted_data":    map[string]any{"user_id": 123},
				},
				Status:    "success",
				Timestamp: time.Now().UTC(),
			},
		},
		TriggerData: map[string]any{
			"webhook": map[string]any{
				"path":    "/api/webhook",
				"method":  "POST",
				"payload": map[string]any{"user_id": 123, "action": "create"},
			},
		},
		Variables: map[string]any{
			"current_user": "test_user",
			"debug_info":   map[string]any{"request_id": "req_123", "timestamp": time.Now().UTC()},
		},
		Metadata: map[string]any{
			"execution_start":  time.Now().UTC(),
			"workflow_version": "1.0.0",
		},
		CreatedAt: time.Now().UTC(),
	}

	// Save execution context
	err := execRepo.SaveExecutionContext(ctx, execCtx)
	require.NoError(t, err)

	// Step 7: Test input coordination for multi-input nodes
	inputRepo := p.InputCoordinationRepository()

	// Simulate input coordination for api_call node (waiting for validate_data results)
	inputState := &models.NodeInputState{
		NodeID:          "api_call",
		ExecutionID:     execCtx.ID,
		NodeExecutionID: uuid.New().String(),
		ReceivedInputs: map[string]models.NodeResult{
			"validated_input": {
				NodeID: "validate_data",
				Data: map[string]any{
					"user_id": 123,
					"action":  "create",
					"valid":   true,
				},
				Status:    "success",
				Timestamp: time.Now().UTC(),
			},
		},
		Requirements: models.InputRequirements{
			RequiredPorts: []string{"validated_input"},
			OptionalPorts: []string{"config"},
			WaitMode:      models.WaitModeAll,
			Timeout:       &[]time.Duration{30 * time.Second}[0],
		},
		CreatedAt:     time.Now().UTC(),
		LastUpdatedAt: time.Now().UTC(),
	}

	err = inputRepo.SaveInputState(ctx, inputState)
	require.NoError(t, err)

	// Step 8: Complete the execution and update context
	completedTime := time.Now().UTC()
	execCtx.Status = models.ExecutionStatusCompleted
	execCtx.CompletedAt = &completedTime
	execCtx.NodeResults["api_call"] = models.NodeResult{
		NodeID: "api_call",
		Data: map[string]any{
			"response": map[string]any{"status": "success", "id": "proc_456"},
			"metrics":  map[string]any{"duration_ms": 150, "retry_count": 0},
		},
		Status:    "success",
		Timestamp: time.Now().UTC(),
	}
	execCtx.NodeResults["log_result"] = models.NodeResult{
		NodeID: "log_result",
		Data: map[string]any{
			"logged_message": "Processing complete: success",
			"log_level":      "info",
		},
		Status:    "success",
		Timestamp: time.Now().UTC(),
	}

	err = execRepo.UpdateExecutionContext(ctx, execCtx)
	require.NoError(t, err)

	// Clean up input coordination state after successful execution
	err = inputRepo.DeleteInputState(ctx, inputState.NodeExecutionID)
	require.NoError(t, err)

	// Step 8: Query executions by workflow and status
	workflowExecutions, err := execRepo.GetExecutionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Len(t, workflowExecutions, 1)
	assert.Equal(t, execCtx.ID, workflowExecutions[0].ID)

	completedExecutions, err := execRepo.GetExecutionsByStatus(ctx, models.ExecutionStatusCompleted)
	require.NoError(t, err)
	assert.Len(t, completedExecutions, 1)
	assert.Equal(t, execCtx.ID, completedExecutions[0].ID)

	return execCtx
}

func testWorkflowModifications(t *testing.T, p *postgresql.Persistence, ctx context.Context, workflow *models.Workflow) {
	t.Helper()

	// Step 9: Modify workflow using individual repository operations
	nodeRepo := p.NodeRepository()
	connRepo := p.ConnectionRepository()

	// Add a new node
	newNode := &models.WorkflowNode{
		ID:        "notification",
		Type:      "log",
		Category:  models.CategoryTypeAction,
		Name:      "Send Notification",
		Config:    map[string]any{"message": "Workflow completed for user {{.user_id}}", "level": "info"},
		Enabled:   true,
		PositionX: 900,
		PositionY: 100,
	}

	err := nodeRepo.SaveNode(ctx, workflow.ID, newNode)
	require.NoError(t, err)

	// Add connection to new node
	newConnection := &models.Connection{
		ID:         "conn6",
		SourcePort: "log_result:success",
		TargetPort: "notification:input",
	}

	err = connRepo.SaveConnection(ctx, workflow.ID, newConnection)
	require.NoError(t, err)

	// Verify the modifications
	updatedNodes, err := nodeRepo.GetNodesByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Len(t, updatedNodes, 6) // Original 5 + 1 new

	updatedConnections, err := connRepo.GetConnectionsByWorkflow(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Len(t, updatedConnections, 6) // Original 5 + 1 new
}

func testCleanupOperations(t *testing.T, p *postgresql.Persistence, ctx context.Context, workflow *models.Workflow, execCtx *models.ExecutionContext) {
	t.Helper()

	// Step 10: Test cleanup operations
	inputRepo := p.InputCoordinationRepository()
	workflowRepo := p.WorkflowRepository()

	// Clean up old input coordination states
	err := inputRepo.CleanupExpiredStates(ctx, 1*time.Hour)
	require.NoError(t, err)

	// Soft delete the workflow
	err = workflowRepo.Delete(ctx, workflow.ID)
	require.NoError(t, err)

	// Verify workflow is soft deleted
	deletedWorkflow, err := workflowRepo.GetByID(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Nil(t, deletedWorkflow)
}

func TestRepositoryIntegration_MultipleWorkflowsExecution(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	// Create multiple workflows
	workflows := make([]*models.Workflow, 3)

	for i := range 3 {
		sourceID := uuid.New().String()
		workflows[i] = &models.Workflow{
			ID:          uuid.New().String(),
			Name:        fmt.Sprintf("Test Workflow %d", i+1),
			Description: fmt.Sprintf("Description for workflow %d", i+1),
			Nodes: []*models.WorkflowNode{
				{
					ID:         fmt.Sprintf("trigger_%d", i+1),
					Type:       "trigger:scheduler",
					Category:   models.CategoryTypeTrigger,
					Name:       fmt.Sprintf("Schedule Trigger %d", i+1),
					Config:     map[string]any{"cron": fmt.Sprintf("0 %d * * *", i)},
					SourceID:   &sourceID,
					ProviderID: &[]string{"scheduler"}[0],
					EventType:  &[]string{"schedule_due"}[0],
					Enabled:    true,
				},
				{
					ID:       fmt.Sprintf("action_%d", i+1),
					Type:     "log",
					Category: models.CategoryTypeAction,
					Name:     fmt.Sprintf("Log Action %d", i+1),
					Config:   map[string]any{"message": fmt.Sprintf("Workflow %d executed", i+1)},
					Enabled:  true,
				},
			},
			Connections: []*models.Connection{
				{
					ID:         fmt.Sprintf("conn_%d", i+1),
					SourcePort: fmt.Sprintf("trigger_%d:output", i+1),
					TargetPort: fmt.Sprintf("action_%d:input", i+1),
				},
			},
			Status: models.WorkflowStatusPublished,
			Owner:  "test-user",
		}

		// Save workflow
		err := p.WorkflowRepository().Save(ctx, workflows[i])
		require.NoError(t, err)
	}

	// Create execution contexts for each workflow
	execContexts := make([]*models.ExecutionContext, 3)
	for i, workflow := range workflows {
		execContexts[i] = &models.ExecutionContext{
			ID:         uuid.New().String(),
			WorkflowID: workflow.ID,
			Status:     []models.ExecutionStatus{models.ExecutionStatusRunning, models.ExecutionStatusCompleted, models.ExecutionStatusFailed}[i],
			NodeResults: map[string]models.NodeResult{
				fmt.Sprintf("trigger_%d", i+1): {
					NodeID:    fmt.Sprintf("trigger_%d", i+1),
					Data:      map[string]any{"triggered_at": time.Now().UTC()},
					Status:    "success",
					Timestamp: time.Now().UTC(),
				},
			},
			TriggerData: map[string]any{
				"scheduler": map[string]any{"cron": fmt.Sprintf("0 %d * * *", i), "execution_time": time.Now().UTC()},
			},
			CreatedAt: time.Now().UTC().Add(time.Duration(-i) * time.Hour), // Different creation times
		}

		if execContexts[i].Status == models.ExecutionStatusCompleted {
			completedTime := time.Now().UTC()
			execContexts[i].CompletedAt = &completedTime
		}

		err := p.ExecutionContextRepository().SaveExecutionContext(ctx, execContexts[i])
		require.NoError(t, err)
	}

	// Test querying across multiple workflows and executions
	result, err := p.WorkflowRepository().ListWorkflows(ctx, persistence.ListWorkflowsOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	assert.Len(t, result.Workflows, 3)

	// Query executions by status
	runningExecs, err := p.ExecutionContextRepository().GetExecutionsByStatus(ctx, models.ExecutionStatusRunning)
	require.NoError(t, err)
	assert.Len(t, runningExecs, 1)

	completedExecs, err := p.ExecutionContextRepository().GetExecutionsByStatus(ctx, models.ExecutionStatusCompleted)
	require.NoError(t, err)
	assert.Len(t, completedExecs, 1)

	failedExecs, err := p.ExecutionContextRepository().GetExecutionsByStatus(ctx, models.ExecutionStatusFailed)
	require.NoError(t, err)
	assert.Len(t, failedExecs, 1)

	// Query executions by specific workflow
	workflow1Execs, err := p.ExecutionContextRepository().GetExecutionsByWorkflow(ctx, workflows[0].ID)
	require.NoError(t, err)
	assert.Len(t, workflow1Execs, 1)
	assert.Equal(t, execContexts[0].ID, workflow1Execs[0].ID)

	// Test trigger node finding across multiple workflows
	// All workflows use scheduler provider with schedule_due event
	allTriggers, err := p.NodeRepository().FindTriggerNodesBySourceEventAndProvider(ctx, "", "schedule_due", "scheduler", models.WorkflowStatusPublished)
	require.NoError(t, err)
	assert.Len(t, allTriggers, 0) // Should be 0 because we're searching for empty sourceID

	// Test node operations across workflows
	totalNodes := 0

	for _, workflow := range workflows {
		nodes, err := p.NodeRepository().GetNodesByWorkflow(ctx, workflow.ID)
		require.NoError(t, err)

		totalNodes += len(nodes)
	}

	assert.Equal(t, 6, totalNodes) // 2 nodes per workflow * 3 workflows

	totalConnections := 0

	for _, workflow := range workflows {
		connections, err := p.ConnectionRepository().GetConnectionsByWorkflow(ctx, workflow.ID)
		require.NoError(t, err)

		totalConnections += len(connections)
	}

	assert.Equal(t, 3, totalConnections) // 1 connection per workflow * 3 workflows
}
