package postgresql_test

import (
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestWorkflow(t *testing.T) *models.Workflow {
	t.Helper()

	return &models.Workflow{
		ID:          uuid.New().String(),
		Name:        "Execution Test Workflow",
		Description: "A workflow for testing executions",
		Nodes: []*models.WorkflowNode{
			{ID: "action1", Type: logNodeType, Category: models.CategoryTypeAction, Name: "action1", Config: map[string]any{messageKey: "execution test"}},
		},
		Status: models.WorkflowStatusPublished,
	}
}

func createTestExecutionContext(t *testing.T, workflowID string) *models.ExecutionContext {
	t.Helper()

	return &models.ExecutionContext{
		ID:         uuid.New().String(),
		WorkflowID: workflowID,
		Status:     models.ExecutionStatusRunning,
		NodeResults: map[string]models.NodeResult{
			"node1": {
				NodeID:    "node1",
				Data:      map[string]any{"result": "success", "value": 42},
				Status:    "success",
				Timestamp: time.Now().UTC(),
			},
			"node2": {
				NodeID:    "node2",
				Data:      map[string]any{"partial": "data"},
				Status:    "failed",
				Error:     "some error occurred",
				Timestamp: time.Now().UTC(),
			},
		},
		TriggerData: map[string]any{
			"trigger_source": "webhook",
			"payload":        map[string]any{"user_id": "123", "action": "create"},
		},
		Variables: map[string]any{
			"user_name": "test_user",
			"timeout":   30,
			"enabled":   true,
		},
		Metadata: map[string]any{
			"version":     "1.0.0",
			"environment": "test",
			"tags":        []string{"test", "execution"},
		},
		ErrorMessage: "",
		CreatedAt:    time.Now().UTC(),
		CompletedAt:  nil,
	}
}

func TestExecutionContextRepository_SaveAndGetExecutionContext(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	// Create and save a workflow first
	workflow := createTestWorkflow(t)
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	execRepo := p.ExecutionContextRepository()

	// Create execution context
	execCtx := createTestExecutionContext(t, workflow.ID)

	// Test SaveExecutionContext
	err = execRepo.SaveExecutionContext(ctx, execCtx)
	require.NoError(t, err)

	// Test GetExecutionContext
	retrieved, err := execRepo.GetExecutionContext(ctx, execCtx.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, execCtx.ID, retrieved.ID)
	assert.Equal(t, execCtx.WorkflowID, retrieved.WorkflowID)
	assert.Equal(t, execCtx.Status, retrieved.Status)
	assert.Equal(t, execCtx.ErrorMessage, retrieved.ErrorMessage)

	// Verify NodeResults
	assert.Len(t, retrieved.NodeResults, 2)
	assert.Equal(t, execCtx.NodeResults["node1"].Status, retrieved.NodeResults["node1"].Status)
	assert.Equal(t, execCtx.NodeResults["node1"].Data["result"], retrieved.NodeResults["node1"].Data["result"])
	assert.Equal(t, execCtx.NodeResults["node2"].Status, retrieved.NodeResults["node2"].Status)
	assert.Equal(t, execCtx.NodeResults["node2"].Error, retrieved.NodeResults["node2"].Error)

	// Verify TriggerData
	assert.Equal(t, execCtx.TriggerData["trigger_source"], retrieved.TriggerData["trigger_source"])
	triggerPayload := retrieved.TriggerData["payload"].(map[string]any)
	assert.Equal(t, "123", triggerPayload["user_id"])

	// Verify Variables
	assert.Equal(t, execCtx.Variables["user_name"], retrieved.Variables["user_name"])
	assert.Equal(t, float64(30), retrieved.Variables["timeout"]) // JSON unmarshals numbers as float64
	assert.Equal(t, execCtx.Variables["enabled"], retrieved.Variables["enabled"])

	// Verify Metadata
	assert.Equal(t, execCtx.Metadata["version"], retrieved.Metadata["version"])
	assert.Equal(t, execCtx.Metadata["environment"], retrieved.Metadata["environment"])
	tags := retrieved.Metadata["tags"].([]any)
	assert.Len(t, tags, 2)
	assert.Contains(t, tags, "test")
	assert.Contains(t, tags, "execution")
}

func TestExecutionContextRepository_UpdateExecutionContext(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	// Create and save a workflow first
	workflow := createTestWorkflow(t)
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	execRepo := p.ExecutionContextRepository()

	// Create and save execution context
	execCtx := createTestExecutionContext(t, workflow.ID)
	err = execRepo.SaveExecutionContext(ctx, execCtx)
	require.NoError(t, err)

	// Update execution context
	completedTime := time.Now().UTC()
	execCtx.Status = models.ExecutionStatusCompleted
	execCtx.CompletedAt = &completedTime
	execCtx.NodeResults["node3"] = models.NodeResult{
		NodeID:    "node3",
		Data:      map[string]any{"final": "result"},
		Status:    "success",
		Timestamp: time.Now().UTC(),
	}
	execCtx.ErrorMessage = ""

	err = execRepo.UpdateExecutionContext(ctx, execCtx)
	require.NoError(t, err)

	// Verify update
	updated, err := execRepo.GetExecutionContext(ctx, execCtx.ID)
	require.NoError(t, err)
	require.NotNil(t, updated)

	assert.Equal(t, models.ExecutionStatusCompleted, updated.Status)
	assert.NotNil(t, updated.CompletedAt)
	assert.Len(t, updated.NodeResults, 3)
	assert.Equal(t, "result", updated.NodeResults["node3"].Data["final"])
}

func TestExecutionContextRepository_GetExecutionsByWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	// Create and save workflows
	workflow1 := createTestWorkflow(t)
	workflow1.Name = "Workflow 1"
	err := p.WorkflowRepository().Save(ctx, workflow1)
	require.NoError(t, err)

	workflow2 := createTestWorkflow(t)
	workflow2.Name = "Workflow 2"
	err = p.WorkflowRepository().Save(ctx, workflow2)
	require.NoError(t, err)

	execRepo := p.ExecutionContextRepository()

	// Create execution contexts for workflow1
	execCtx1 := createTestExecutionContext(t, workflow1.ID)
	execCtx1.CreatedAt = time.Now().UTC().Add(-2 * time.Hour)
	err = execRepo.SaveExecutionContext(ctx, execCtx1)
	require.NoError(t, err)

	execCtx2 := createTestExecutionContext(t, workflow1.ID)
	execCtx2.Status = models.ExecutionStatusCompleted
	execCtx2.CreatedAt = time.Now().UTC().Add(-1 * time.Hour)
	err = execRepo.SaveExecutionContext(ctx, execCtx2)
	require.NoError(t, err)

	// Create execution context for workflow2
	execCtx3 := createTestExecutionContext(t, workflow2.ID)
	execCtx3.Status = models.ExecutionStatusFailed
	err = execRepo.SaveExecutionContext(ctx, execCtx3)
	require.NoError(t, err)

	// Test GetExecutionsByWorkflow
	executions, err := execRepo.GetExecutionsByWorkflow(ctx, workflow1.ID, 10)
	require.NoError(t, err)

	assert.Len(t, executions, 2)

	// Should be ordered by created_at DESC (newest first)
	assert.Equal(t, execCtx2.ID, executions[0].ID)
	assert.Equal(t, execCtx1.ID, executions[1].ID)

	newest, err := execRepo.GetExecutionsByWorkflow(ctx, workflow1.ID, 1)
	require.NoError(t, err)
	require.Len(t, newest, 1)
	assert.Equal(t, execCtx2.ID, newest[0].ID)

	// Test with workflow that has one execution
	executions, err = execRepo.GetExecutionsByWorkflow(ctx, workflow2.ID, 10)
	require.NoError(t, err)

	assert.Len(t, executions, 1)
	assert.Equal(t, execCtx3.ID, executions[0].ID)
	assert.Equal(t, models.ExecutionStatusFailed, executions[0].Status)

	// Test with workflow that has no executions
	workflow3 := createTestWorkflow(t)
	workflow3.Name = "Workflow 3"
	err = p.WorkflowRepository().Save(ctx, workflow3)
	require.NoError(t, err)

	executions, err = execRepo.GetExecutionsByWorkflow(ctx, workflow3.ID, 10)
	require.NoError(t, err)

	assert.Len(t, executions, 0)
}

func TestExecutionContextRepository_GetExecutionsByStatus(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	// Create and save a workflow
	workflow := createTestWorkflow(t)
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	execRepo := p.ExecutionContextRepository()

	// Create execution contexts with different statuses
	execCtx1 := createTestExecutionContext(t, workflow.ID)
	execCtx1.Status = models.ExecutionStatusRunning
	execCtx1.CreatedAt = time.Now().UTC().Add(-3 * time.Hour)
	err = execRepo.SaveExecutionContext(ctx, execCtx1)
	require.NoError(t, err)

	execCtx2 := createTestExecutionContext(t, workflow.ID)
	execCtx2.Status = models.ExecutionStatusRunning
	execCtx2.CreatedAt = time.Now().UTC().Add(-2 * time.Hour)
	err = execRepo.SaveExecutionContext(ctx, execCtx2)
	require.NoError(t, err)

	execCtx3 := createTestExecutionContext(t, workflow.ID)
	execCtx3.Status = models.ExecutionStatusCompleted
	execCtx3.CreatedAt = time.Now().UTC().Add(-1 * time.Hour)
	err = execRepo.SaveExecutionContext(ctx, execCtx3)
	require.NoError(t, err)

	execCtx4 := createTestExecutionContext(t, workflow.ID)
	execCtx4.Status = models.ExecutionStatusFailed
	execCtx4.ErrorMessage = "Task failed"
	err = execRepo.SaveExecutionContext(ctx, execCtx4)
	require.NoError(t, err)

	// Test GetExecutionsByStatus - running
	runningExecutions, err := execRepo.GetExecutionsByStatus(ctx, models.ExecutionStatusRunning)
	require.NoError(t, err)

	assert.Len(t, runningExecutions, 2)

	// Should be ordered by created_at DESC (newest first)
	assert.Equal(t, execCtx2.ID, runningExecutions[0].ID)
	assert.Equal(t, execCtx1.ID, runningExecutions[1].ID)

	// Test GetExecutionsByStatus - completed
	completedExecutions, err := execRepo.GetExecutionsByStatus(ctx, models.ExecutionStatusCompleted)
	require.NoError(t, err)

	assert.Len(t, completedExecutions, 1)
	assert.Equal(t, execCtx3.ID, completedExecutions[0].ID)

	// Test GetExecutionsByStatus - failed
	failedExecutions, err := execRepo.GetExecutionsByStatus(ctx, models.ExecutionStatusFailed)
	require.NoError(t, err)

	assert.Len(t, failedExecutions, 1)
	assert.Equal(t, execCtx4.ID, failedExecutions[0].ID)
	assert.Equal(t, "Task failed", failedExecutions[0].ErrorMessage)

	// Test GetExecutionsByStatus - status with no executions
	cancelledExecutions, err := execRepo.GetExecutionsByStatus(ctx, models.ExecutionStatusCancelled)
	require.NoError(t, err)

	assert.Len(t, cancelledExecutions, 0)
}

func TestExecutionContextRepository_ComplexDataTypes(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	// Create and save a workflow
	workflow := createTestWorkflow(t)
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	execRepo := p.ExecutionContextRepository()

	// Create execution context with complex nested data
	execCtx := &models.ExecutionContext{
		ID:         uuid.New().String(),
		WorkflowID: workflow.ID,
		Status:     models.ExecutionStatusRunning,
		NodeResults: map[string]models.NodeResult{
			"api_call": {
				NodeID: "api_call",
				Status: "success",
				Data: map[string]any{
					"users": []map[string]any{
						{"id": 1, "name": "Alice", "roles": []string{"admin", "user"}},
						{"id": 2, "name": "Bob", "roles": []string{"user"}},
					},
					"pagination": map[string]any{
						"page":       1,
						"per_page":   10,
						"total":      2,
						"has_more":   false,
						"next_token": nil,
					},
				},
				Timestamp: time.Now().UTC(),
			},
			"validation": {
				NodeID: "validation",
				Status: "failed",
				Error:  "Validation failed: missing required field 'email'",
				Data: map[string]any{
					"errors": []map[string]any{
						{"field": "email", "code": "required", "message": "Email is required"},
						{"field": "age", "code": "invalid", "message": "Age must be positive"},
					},
				},
				Timestamp: time.Now().UTC(),
			},
		},
		TriggerData: map[string]any{
			"webhook": map[string]any{
				"headers": map[string]any{
					"Content-Type":   "application/json",
					"User-Agent":     "TestClient/1.0",
					"Authorization":  "Bearer token123",
					"Custom-Headers": []string{"header1", "header2"},
				},
				"body": map[string]any{
					"data": map[string]any{
						"nested": map[string]any{
							"deep": map[string]any{
								"value": "deeply nested data",
								"array": []any{1, 2, 3, "string", true, nil},
							},
						},
					},
				},
			},
		},
		Variables: map[string]any{
			"config": map[string]any{
				"retries":   3,
				"timeout":   30.5,
				"endpoints": []string{"https://api1.com", "https://api2.com"},
				"features": map[string]any{
					"logging":    true,
					"monitoring": false,
					"debug_mode": nil,
					"batch_size": 100,
				},
			},
		},
		Metadata: map[string]any{
			"execution_plan": []map[string]any{
				{"step": 1, "action": "validate", "duration_ms": 150},
				{"step": 2, "action": "process", "duration_ms": 320},
				{"step": 3, "action": "respond", "duration_ms": nil},
			},
		},
		CreatedAt: time.Now().UTC(),
	}

	// Save execution context
	err = execRepo.SaveExecutionContext(ctx, execCtx)
	require.NoError(t, err)

	// Retrieve and verify complex data
	retrieved, err := execRepo.GetExecutionContext(ctx, execCtx.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	// Verify complex NodeResults
	apiResult := retrieved.NodeResults["api_call"]
	assert.Equal(t, "success", apiResult.Status)

	users := apiResult.Data["users"].([]any)
	assert.Len(t, users, 2)

	firstUser := users[0].(map[string]any)
	assert.Equal(t, float64(1), firstUser["id"]) // JSON numbers are float64
	assert.Equal(t, "Alice", firstUser["name"])

	roles := firstUser["roles"].([]any)
	assert.Contains(t, roles, "admin")
	assert.Contains(t, roles, "user")

	// Verify nested TriggerData
	webhookData := retrieved.TriggerData["webhook"].(map[string]any)
	headers := webhookData["headers"].(map[string]any)
	assert.Equal(t, "application/json", headers["Content-Type"])

	customHeaders := headers["Custom-Headers"].([]any)
	assert.Contains(t, customHeaders, "header1")
	assert.Contains(t, customHeaders, "header2")

	// Verify deeply nested data
	body := webhookData["body"].(map[string]any)
	data := body["data"].(map[string]any)
	nested := data["nested"].(map[string]any)
	deep := nested["deep"].(map[string]any)
	assert.Equal(t, "deeply nested data", deep["value"])

	deepArray := deep["array"].([]any)
	assert.Len(t, deepArray, 6)
	assert.Contains(t, deepArray, float64(1))
	assert.Contains(t, deepArray, "string")
	assert.Contains(t, deepArray, true)
	assert.Contains(t, deepArray, nil)
}

func TestExecutionContextRepository_ErrorCases(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	execRepo := p.ExecutionContextRepository()

	// Test getting non-existent execution context
	execCtx, err := execRepo.GetExecutionContext(ctx, "non-existent-id")
	require.Error(t, err)
	assert.Nil(t, execCtx)
	assert.Contains(t, err.Error(), "execution context not found")

	// Test updating non-existent execution context
	nonExistentCtx := &models.ExecutionContext{
		ID:         "non-existent-id",
		WorkflowID: uuid.New().String(),
		Status:     models.ExecutionStatusRunning,
	}

	err = execRepo.UpdateExecutionContext(ctx, nonExistentCtx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "execution context not found")

	// Test saving execution context with non-existent workflow
	invalidExecCtx := &models.ExecutionContext{
		ID:          uuid.New().String(),
		WorkflowID:  uuid.New().String(), // Use valid UUID format
		Status:      models.ExecutionStatusRunning,
		NodeResults: make(map[string]models.NodeResult),
		Variables:   make(map[string]any),
		TriggerData: make(map[string]any),
		Metadata:    make(map[string]any),
		CreatedAt:   time.Now().UTC(),
	}

	err = execRepo.SaveExecutionContext(ctx, invalidExecCtx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "violates foreign key constraint")
}
