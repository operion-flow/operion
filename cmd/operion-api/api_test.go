package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/dukex/operion/pkg/mocks"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/file"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/services"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestApp(tempDir string) *fiber.App {
	persistence := file.NewPersistence(tempDir)

	// Create mock event bus for testing
	mockEventBus := &mocks.MockEventBus{}

	app := NewAPI(
		slog.Default(),
		persistence,
		registry.NewRegistry(slog.Default()),
		mockEventBus,
	)

	return app.App()
}

func TestAPI_RootEndpoint(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "Operion API", string(body))
}

func TestAPI_HealthCheck(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "OK", string(body))
}

func TestAPI_GetWorkflows_Empty(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response struct {
		Workflows   []models.Workflow `json:"workflows"`
		TotalCount  int64             `json:"total_count"`
		HasNextPage bool              `json:"has_next_page"`
	}

	err = json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Workflows)
	assert.Equal(t, int64(0), response.TotalCount)
	assert.False(t, response.HasNextPage)
}

func TestAPI_GetWorkflows_WithData(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	persistence := file.NewPersistence(tempDir)

	// Create test workflows
	workflow1 := &models.Workflow{
		ID:     "test-workflow-1",
		Name:   "Test Workflow 1",
		Status: "active",
		Nodes: []*models.WorkflowNode{
			{
				ID:       "node1",
				Name:     "Log Node",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"message": "Test message",
				},
				Enabled: true,
			},
		},
		Connections: []*models.Connection{},
		Variables: map[string]any{
			"test_var": "test_value",
		},
	}

	workflow2 := &models.Workflow{
		ID:     "test-workflow-2",
		Name:   "Test Workflow 2",
		Status: "inactive",
		Nodes: []*models.WorkflowNode{
			{
				ID:       "node1",
				Name:     "Transform Node",
				Type:     "transform",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"expression": "{ \"result\": \"transformed\" }",
				},
				Enabled: true,
			},
		},
		Connections: []*models.Connection{},
		Variables:   map[string]any{},
	}

	// Save workflows
	repo := services.NewWorkflow(persistence)
	createdWorkflow1, err := repo.Create(t.Context(), workflow1)
	require.NoError(t, err)
	createdWorkflow2, err := repo.Create(t.Context(), workflow2)
	require.NoError(t, err)

	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response struct {
		Workflows   []models.Workflow `json:"workflows"`
		TotalCount  int64             `json:"total_count"`
		HasNextPage bool              `json:"has_next_page"`
	}

	err = json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)
	assert.Len(t, response.Workflows, 2)
	assert.Equal(t, int64(2), response.TotalCount)
	assert.False(t, response.HasNextPage)

	// Verify workflow data
	workflowIDs := []string{response.Workflows[0].ID, response.Workflows[1].ID}
	assert.Contains(t, workflowIDs, createdWorkflow1.ID)
	assert.Contains(t, workflowIDs, createdWorkflow2.ID)
}

func TestAPI_GetWorkflow_Success(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	persistence := file.NewPersistence(tempDir)

	// Create test workflow
	workflow1 := &models.Workflow{
		ID:     "test-workflow-specific",
		Name:   "Specific Test Workflow",
		Status: "active",
		Nodes: []*models.WorkflowNode{
			{
				ID:       "node1",
				Name:     "HTTP Request Node",
				Type:     "http_request",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"protocol": "https",
					"host":     "api.example.com",
					"path":     "/data",
					"method":   "GET",
				},
				Enabled: true,
			},
		},
		Connections: []*models.Connection{},
		Variables: map[string]any{
			"api_key": "test-key",
		},
	}

	// Save workflow
	repo := services.NewWorkflow(persistence)
	workflowCreated, err := repo.Create(t.Context(), workflow1)
	require.NoError(t, err)

	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/workflows/"+workflowCreated.ID, nil)
	req.Header.Set("Accept", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var returnedWorkflow models.Workflow

	err = json.NewDecoder(resp.Body).Decode(&returnedWorkflow)
	require.NoError(t, err)

	assert.Equal(t, workflowCreated.ID, returnedWorkflow.ID)
	assert.Equal(t, "Specific Test Workflow", returnedWorkflow.Name)
	assert.Equal(t, models.WorkflowStatus("active"), returnedWorkflow.Status)
	assert.Len(t, returnedWorkflow.Nodes, 1)
	assert.Equal(t, "http_request", returnedWorkflow.Nodes[0].Type)
	assert.Equal(t, "test-key", returnedWorkflow.Variables["api_key"])
}

func TestAPI_GetWorkflow_NotFound(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/workflows/non-existent-workflow", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAPI_GetWorkflow_InvalidID(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/workflows/", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	// Should return the list of workflows instead
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAPI_CORS_Headers(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodOptions, "/workflows", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "GET")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestAPI_ContentType_JSON(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	app := setupTestApp(tempDir)

	req := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
}

// nolint: funlen
func TestAPI_Integration_WorkflowLifecycle(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	persistence := file.NewPersistence(tempDir)

	// Create a comprehensive test workflow
	complexWorkflow := &models.Workflow{
		ID:          "integration-test-workflow",
		Name:        "Integration Test Workflow",
		Description: "A comprehensive workflow for integration testing",
		Status:      "active",
		Nodes: []*models.WorkflowNode{
			{
				ID:       "trigger1",
				Name:     "Integration Test Trigger",
				Type:     "trigger:scheduler",
				Category: models.CategoryTypeTrigger,
				Config: map[string]any{
					"schedule": "0 0 * * *",
				},
				SourceID:   &[]string{uuid.New().String()}[0],
				ProviderID: &[]string{"scheduler"}[0],
				EventType:  &[]string{"schedule_due"}[0],
				Enabled:    true,
			},
			{
				ID:       "node1",
				Name:     "Log Initial Message",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"message": "Starting integration test workflow",
				},
				Enabled: true,
			},
			{
				ID:       "node2",
				Name:     "HTTP API Call",
				Type:     "http_request",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"protocol": "https",
					"host":     "httpbin.org",
					"path":     "/json",
					"method":   "GET",
					"timeout":  30,
				},
				Enabled: true,
			},
			{
				ID:       "node3",
				Name:     "Transform Response",
				Type:     "transform",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"expression": "{ \"processed\": true, \"original\": \"{{.node_results.api_call.body}}\" }",
				},
				Enabled: true,
			},
			{
				ID:       "node4",
				Name:     "Log Error",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"message": "API call failed",
				},
				Enabled: true,
			},
			{
				ID:       "node5",
				Name:     "Final Log",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"message": "Integration test completed successfully",
				},
				Enabled: true,
			},
		},
		Connections: []*models.Connection{
			{
				ID:         "conn1",
				SourcePort: "node1:output",
				TargetPort: "node2:input",
			},
			{
				ID:         "conn2",
				SourcePort: "node2:success",
				TargetPort: "node3:input",
			},
			{
				ID:         "conn3",
				SourcePort: "node2:error",
				TargetPort: "node4:input",
			},
			{
				ID:         "conn4",
				SourcePort: "node3:output",
				TargetPort: "node5:input",
			},
		},
		Variables: map[string]any{
			"environment": "test",
			"version":     "1.0.0",
			"config": map[string]any{
				"retry_attempts": 3,
				"timeout":        30,
			},
		},
	}

	// Save workflow
	repo := services.NewWorkflow(persistence)
	workflowCreated, err := repo.Create(t.Context(), complexWorkflow)
	require.NoError(t, err)

	app := setupTestApp(tempDir)

	// Test 1: Fetch all workflows and verify our workflow is there
	req := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response struct {
		Workflows   []models.Workflow `json:"workflows"`
		TotalCount  int64             `json:"total_count"`
		HasNextPage bool              `json:"has_next_page"`
	}

	err = json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)
	assert.Len(t, response.Workflows, 1)
	assert.Equal(t, int64(1), response.TotalCount)
	assert.False(t, response.HasNextPage)
	assert.Equal(t, workflowCreated.ID, response.Workflows[0].ID)

	// Test 2: Fetch specific workflow and verify all details
	req = httptest.NewRequest(http.MethodGet, "/workflows/"+workflowCreated.ID, nil)
	req.Header.Set("Accept", "application/json")
	resp, err = app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var fetchedWorkflow models.Workflow

	err = json.NewDecoder(resp.Body).Decode(&fetchedWorkflow)
	require.NoError(t, err)

	// Verify workflow structure
	assert.Equal(t, workflowCreated.ID, fetchedWorkflow.ID)
	assert.Equal(t, "Integration Test Workflow", fetchedWorkflow.Name)
	assert.Equal(t, "A comprehensive workflow for integration testing", fetchedWorkflow.Description)
	assert.Equal(t, models.WorkflowStatus("active"), fetchedWorkflow.Status)
	assert.Len(t, fetchedWorkflow.Nodes, 6) // Now includes trigger node
	assert.Len(t, fetchedWorkflow.Connections, 4)

	// Verify trigger node (first node)
	triggerNode := fetchedWorkflow.Nodes[0]
	assert.Equal(t, "trigger:scheduler", triggerNode.Type)
	assert.Equal(t, models.CategoryTypeTrigger, triggerNode.Category)
	assert.Equal(t, "scheduler", *triggerNode.ProviderID)
	assert.Equal(t, "schedule_due", *triggerNode.EventType)
	assert.Equal(t, "0 0 * * *", triggerNode.Config["schedule"])

	// Verify action nodes
	logNode := fetchedWorkflow.Nodes[1]
	assert.Equal(t, "log", logNode.Type)
	assert.Equal(t, models.CategoryTypeAction, logNode.Category)
	assert.Equal(t, "Starting integration test workflow", logNode.Config["message"])

	httpNode := fetchedWorkflow.Nodes[2]
	assert.Equal(t, "http_request", httpNode.Type)
	assert.Equal(t, models.CategoryTypeAction, httpNode.Category)
	assert.Equal(t, "https", httpNode.Config["protocol"])
	assert.Equal(t, "httpbin.org", httpNode.Config["host"])

	// Verify variables
	assert.Equal(t, "test", fetchedWorkflow.Variables["environment"])
	assert.Equal(t, "1.0.0", fetchedWorkflow.Variables["version"])

	config, ok := fetchedWorkflow.Variables["config"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(3), config["retry_attempts"], 0.001)
}

// Helper function to create string pointers.
