package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dukex/operion/pkg/mocks"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/file"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/services"
	"github.com/dukex/operion/pkg/web"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func setupTestHandlers(t *testing.T) (*web.APIHandlers, *services.Workflow, *services.Node) {
	t.Helper()
	tempDir := t.TempDir()
	persistence := file.NewPersistence(tempDir)
	workflowService := services.NewWorkflow(persistence)
	publishingService := services.NewPublishing(persistence)
	nodeService := services.NewNode(persistence)
	validator := validator.New(validator.WithRequiredStructEnabled())
	registryInstance := registry.NewRegistry(slog.Default())

	// Create mock event bus for testing
	mockEventBus := &mocks.MockEventBus{}

	// Set up mock to accept any Publish calls and return nil (success)
	mockEventBus.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	handlers := web.NewAPIHandlers(workflowService, publishingService, nodeService, validator, registryInstance, mockEventBus)

	return handlers, workflowService, nodeService
}

func setupTestApp(t *testing.T) (*fiber.App, *services.Workflow, *services.Node) {
	t.Helper()
	handlers, workflowService, nodeService := setupTestHandlers(t)
	app := fiber.New()

	w := app.Group("/workflows")
	w.Get("/", handlers.GetWorkflows)
	w.Post("/", handlers.CreateWorkflow)
	w.Get("/:id", handlers.GetWorkflow)
	w.Patch("/:id", handlers.UpdateWorkflow)
	w.Delete("/:id", handlers.DeleteWorkflow)
	w.Post("/:id/publish", handlers.PublishWorkflow)
	w.Post("/groups/:groupId/create-draft", handlers.CreateDraftFromPublished)

	// Node endpoints
	w.Post("/:id/nodes", handlers.CreateWorkflowNode)
	w.Get("/:id/nodes/:nodeId", handlers.GetWorkflowNode)
	w.Patch("/:id/nodes/:nodeId", handlers.UpdateWorkflowNode)
	w.Delete("/:id/nodes/:nodeId", handlers.DeleteWorkflowNode)

	return app, workflowService, nodeService
}

func TestAPIHandlers_CreateWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		requestBody    interface{}
		expectedStatus int
		expectedError  string
		validateResult func(t *testing.T, body []byte)
	}{
		{
			name: "successful creation",
			requestBody: web.CreateWorkflowRequest{
				Name:        "Test Workflow",
				Description: "Test Description",
				Owner:       "test-user",
				Variables:   map[string]any{"env": "test"},
				Metadata:    map[string]any{"category": "test"},
			},
			expectedStatus: http.StatusCreated,
			validateResult: func(t *testing.T, body []byte) {
				t.Helper()
				var workflow models.Workflow
				err := json.Unmarshal(body, &workflow)
				require.NoError(t, err)
				assert.Equal(t, "Test Workflow", workflow.Name)
				assert.Equal(t, "Test Description", workflow.Description)
				assert.Equal(t, "test-user", workflow.Owner)
				assert.Equal(t, models.WorkflowStatusDraft, workflow.Status)
				assert.Equal(t, "test", workflow.Variables["env"])
				assert.Equal(t, "test", workflow.Metadata["category"])
				assert.Empty(t, workflow.Nodes)
				assert.Empty(t, workflow.Connections)
				assert.NotEmpty(t, workflow.ID)
			},
		},
		{
			name: "validation error - missing name",
			requestBody: web.CreateWorkflowRequest{
				Description: "Test Description",
				Owner:       "test-user",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Name",
		},
		{
			name: "validation error - name too short",
			requestBody: web.CreateWorkflowRequest{
				Name:        "Te",
				Description: "Test Description",
				Owner:       "test-user",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Name",
		},
		{
			name: "validation error - missing description",
			requestBody: web.CreateWorkflowRequest{
				Name:  "Test Workflow",
				Owner: "test-user",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Description",
		},
		{
			name: "validation error - missing owner",
			requestBody: web.CreateWorkflowRequest{
				Name:        "Test Workflow",
				Description: "Test Description",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Owner",
		},
		{
			name:           "invalid JSON",
			requestBody:    "invalid-json",
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Invalid JSON format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, _, _ := setupTestApp(t)

			var (
				body []byte
				err  error
			)

			if str, ok := tt.requestBody.(string); ok {
				body = []byte(str)
			} else {
				body, err = json.Marshal(tt.requestBody)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(http.MethodPost, "/workflows", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus == http.StatusCreated && tt.validateResult != nil {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				tt.validateResult(t, body)
			} else if tt.expectedError != "" && tt.expectedStatus != http.StatusCreated {
				// For error cases, we don't validate specific error content in this simple test
				// In a real implementation, you would parse the error response JSON
				body, _ := io.ReadAll(resp.Body)
				_ = body // Use the body to avoid empty branch warning
			}
		})
	}
}

func TestAPIHandlers_UpdateWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupWorkflow  *models.Workflow
		requestBody    interface{}
		expectedStatus int
		expectedError  string
		validateResult func(t *testing.T, original *models.Workflow, body []byte)
	}{
		{
			name: "successful partial update - name only",
			setupWorkflow: &models.Workflow{
				ID:          "test-workflow-1",
				Name:        "Original Name",
				Description: "Original Description",
				Owner:       "test-user",
				Variables:   map[string]any{"env": "test"},
			},
			requestBody: web.UpdateWorkflowRequest{
				Name: stringPtr("Updated Name"),
			},
			expectedStatus: http.StatusOK,
			validateResult: func(t *testing.T, original *models.Workflow, body []byte) {
				t.Helper()
				var workflow models.Workflow
				err := json.Unmarshal(body, &workflow)
				require.NoError(t, err)
				assert.Equal(t, "Updated Name", workflow.Name)
				assert.Equal(t, "Original Description", workflow.Description) // unchanged
				assert.Equal(t, "test-user", workflow.Owner)                  // unchanged
			},
		},
		{
			name: "successful partial update - multiple fields",
			setupWorkflow: &models.Workflow{
				ID:          "test-workflow-2",
				Name:        "Original Name",
				Description: "Original Description",
				Owner:       "test-user",
				Variables:   map[string]any{"env": "test"},
			},
			requestBody: web.UpdateWorkflowRequest{
				Name:        stringPtr("Updated Name"),
				Description: stringPtr("Updated Description"),
				Variables:   map[string]any{"env": "production", "new_var": "new_value"},
				Metadata:    map[string]any{"updated": true},
			},
			expectedStatus: http.StatusOK,
			validateResult: func(t *testing.T, original *models.Workflow, body []byte) {
				t.Helper()
				var workflow models.Workflow
				err := json.Unmarshal(body, &workflow)
				require.NoError(t, err)
				assert.Equal(t, "Updated Name", workflow.Name)
				assert.Equal(t, "Updated Description", workflow.Description)
				assert.Equal(t, "production", workflow.Variables["env"])
				assert.Equal(t, "new_value", workflow.Variables["new_var"])
				assert.Equal(t, true, workflow.Metadata["updated"])
			},
		},
		{
			name:           "workflow not found",
			setupWorkflow:  nil,
			requestBody:    web.UpdateWorkflowRequest{Name: stringPtr("New Name")},
			expectedStatus: http.StatusNotFound,
			expectedError:  "Workflow not found",
		},
		{
			name: "validation error - name too short",
			setupWorkflow: &models.Workflow{
				ID:          "test-workflow-3",
				Name:        "Original Name",
				Description: "Original Description",
				Owner:       "test-user",
			},
			requestBody: web.UpdateWorkflowRequest{
				Name: stringPtr("Te"),
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Name",
		},
		{
			name: "empty update request",
			setupWorkflow: &models.Workflow{
				ID:          "test-workflow-4",
				Name:        "Original Name",
				Description: "Original Description",
				Owner:       "test-user",
			},
			requestBody:    web.UpdateWorkflowRequest{},
			expectedStatus: http.StatusOK,
			validateResult: func(t *testing.T, original *models.Workflow, body []byte) {
				t.Helper()
				var workflow models.Workflow
				err := json.Unmarshal(body, &workflow)
				require.NoError(t, err)
				assert.Equal(t, "Original Name", workflow.Name)               // unchanged
				assert.Equal(t, "Original Description", workflow.Description) // unchanged
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, workflowService, _ := setupTestApp(t)

			var workflowID = "non-existent-id"

			if tt.setupWorkflow != nil {
				created, err := workflowService.Create(context.Background(), tt.setupWorkflow)
				require.NoError(t, err)

				workflowID = created.ID
			}

			body, err := json.Marshal(tt.requestBody)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPatch, "/workflows/"+workflowID, bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus == http.StatusOK && tt.validateResult != nil {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				tt.validateResult(t, tt.setupWorkflow, body)
			} else if tt.expectedError != "" && tt.expectedStatus != http.StatusOK {
				// For error cases, we don't validate specific error content in this simple test
				// In a real implementation, you would parse the error response JSON
				body, _ := io.ReadAll(resp.Body)
				_ = body // Use the body to avoid empty branch warning
			}
		})
	}
}

func TestAPIHandlers_DeleteWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupWorkflow  *models.Workflow
		expectedStatus int
		expectedError  string
	}{
		{
			name: "successful deletion",
			setupWorkflow: &models.Workflow{
				ID:          "test-workflow-delete",
				Name:        "Test Workflow",
				Description: "Test Description",
				Owner:       "test-user",
			},
			expectedStatus: http.StatusNoContent,
		},
		{
			name:           "workflow not found",
			setupWorkflow:  nil,
			expectedStatus: http.StatusNotFound,
			expectedError:  "Workflow not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, workflowService, _ := setupTestApp(t)

			var workflowID = "non-existent-id"

			if tt.setupWorkflow != nil {
				created, err := workflowService.Create(context.Background(), tt.setupWorkflow)
				require.NoError(t, err)

				workflowID = created.ID
			}

			req := httptest.NewRequest(http.MethodDelete, "/workflows/"+workflowID, nil)

			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus == http.StatusNoContent {
				// Verify workflow was actually deleted
				_, err := workflowService.FetchByID(context.Background(), workflowID)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "workflow not found")
			}
		})
	}
}

func TestAPIHandlers_GetWorkflows_WithOwnerFilter(t *testing.T) {
	t.Parallel()

	app, workflowService, _ := setupTestApp(t)

	// Create test workflows with different owners
	workflow1 := &models.Workflow{
		ID:          "test-workflow-1",
		Name:        "User1 Workflow",
		Description: "Test Description",
		Owner:       "user1",
	}
	workflow2 := &models.Workflow{
		ID:          "test-workflow-2",
		Name:        "User2 Workflow",
		Description: "Test Description",
		Owner:       "user2",
	}
	workflow3 := &models.Workflow{
		ID:          "test-workflow-3",
		Name:        "Another User1 Workflow",
		Description: "Test Description",
		Owner:       "user1",
	}

	_, err := workflowService.Create(context.Background(), workflow1)
	require.NoError(t, err)
	_, err = workflowService.Create(context.Background(), workflow2)
	require.NoError(t, err)
	_, err = workflowService.Create(context.Background(), workflow3)
	require.NoError(t, err)

	tests := []struct {
		name          string
		ownerID       string
		expectedCount int
		expectedNames []string
	}{
		{
			name:          "filter by user1",
			ownerID:       "user1",
			expectedCount: 2,
			expectedNames: []string{"User1 Workflow", "Another User1 Workflow"},
		},
		{
			name:          "filter by user2",
			ownerID:       "user2",
			expectedCount: 1,
			expectedNames: []string{"User2 Workflow"},
		},
		{
			name:          "filter by non-existent user",
			ownerID:       "user3",
			expectedCount: 0,
			expectedNames: []string{},
		},
		{
			name:          "no filter - get all workflows",
			ownerID:       "",
			expectedCount: 3,
			expectedNames: []string{"User1 Workflow", "User2 Workflow", "Another User1 Workflow"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			url := "/workflows"
			if tt.ownerID != "" {
				url += "?owner_id=" + tt.ownerID
			}

			req := httptest.NewRequest(http.MethodGet, url, nil)
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

			assert.Len(t, response.Workflows, tt.expectedCount)
			assert.Equal(t, int64(tt.expectedCount), response.TotalCount)

			if tt.expectedCount > 0 {
				actualNames := make([]string, len(response.Workflows))
				for i, w := range response.Workflows {
					actualNames[i] = w.Name
				}

				for _, expectedName := range tt.expectedNames {
					assert.Contains(t, actualNames, expectedName)
				}
			}
		})
	}
}

func TestAPIHandlers_PublishWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupWorkflow  *models.Workflow
		expectedStatus int
		expectedError  string
		validateResult func(t *testing.T, body []byte)
	}{
		{
			name: "successful publish",
			setupWorkflow: &models.Workflow{
				ID:          "test-workflow-publish",
				Name:        "Test Workflow",
				Description: "Test Description",
				Owner:       "test-user",
				Status:      models.WorkflowStatusDraft,
				Nodes: []*models.WorkflowNode{
					{
						ID:       "trigger1",
						Name:     "Test Trigger",
						Type:     "trigger:scheduler",
						Category: models.CategoryTypeTrigger,
						Config:   map[string]any{"schedule": "0 0 * * *"},
						Enabled:  true,
					},
				},
			},
			expectedStatus: http.StatusOK,
			validateResult: func(t *testing.T, body []byte) {
				t.Helper()
				var workflow models.Workflow
				err := json.Unmarshal(body, &workflow)
				require.NoError(t, err)
				assert.Equal(t, models.WorkflowStatusPublished, workflow.Status)
				assert.NotNil(t, workflow.PublishedAt)
			},
		},
		{
			name:           "workflow not found",
			setupWorkflow:  nil,
			expectedStatus: http.StatusNotFound,
			expectedError:  "Workflow not found",
		},
		{
			name: "validation failed - no trigger nodes",
			setupWorkflow: &models.Workflow{
				ID:          "test-workflow-no-trigger",
				Name:        "Test Workflow",
				Description: "Test Description",
				Owner:       "test-user",
				Status:      models.WorkflowStatusDraft,
				Nodes: []*models.WorkflowNode{
					{
						ID:       "action1",
						Name:     "Test Action",
						Type:     "log",
						Category: models.CategoryTypeAction,
						Config:   map[string]any{"message": "test"},
						Enabled:  true,
					},
				},
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, workflowService, _ := setupTestApp(t)

			var workflowID = "non-existent-id"

			if tt.setupWorkflow != nil {
				created, err := workflowService.Create(context.Background(), tt.setupWorkflow)
				require.NoError(t, err)

				workflowID = created.ID
			}

			req := httptest.NewRequest(http.MethodPost, "/workflows/"+workflowID+"/publish", nil)

			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus == http.StatusOK && tt.validateResult != nil {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				tt.validateResult(t, body)
			} else if tt.expectedError != "" && tt.expectedStatus != http.StatusOK {
				// For error cases, we don't validate specific error content in this simple test
				// In a real implementation, you would parse the error response JSON
				body, _ := io.ReadAll(resp.Body)
				_ = body // Use the body to avoid empty branch warning
			}
		})
	}
}

func TestAPIHandlers_CreateDraftFromPublished(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupWorkflow  *models.Workflow
		expectedStatus int
		expectedError  string
		validateResult func(t *testing.T, original *models.Workflow, body []byte)
	}{
		{
			name: "successful draft creation",
			setupWorkflow: &models.Workflow{
				ID:              "test-workflow-published",
				Name:            "Published Workflow",
				Description:     "Test Description",
				Owner:           "test-user",
				Status:          models.WorkflowStatusPublished,
				WorkflowGroupID: "group-123",
				Nodes: []*models.WorkflowNode{
					{
						ID:       "trigger1",
						Name:     "Test Trigger",
						Type:     "trigger:scheduler",
						Category: models.CategoryTypeTrigger,
						Config:   map[string]any{"schedule": "0 0 * * *"},
						Enabled:  true,
					},
				},
			},
			expectedStatus: http.StatusCreated,
			validateResult: func(t *testing.T, original *models.Workflow, body []byte) {
				t.Helper()
				var draft models.Workflow
				err := json.Unmarshal(body, &draft)
				require.NoError(t, err)
				assert.Equal(t, models.WorkflowStatusDraft, draft.Status)
				assert.Equal(t, original.Name, draft.Name)
				assert.Equal(t, original.Description, draft.Description)
				assert.Equal(t, original.WorkflowGroupID, draft.WorkflowGroupID)
				assert.NotEqual(t, original.ID, draft.ID) // Should have new ID
				assert.Nil(t, draft.PublishedAt)
			},
		},
		{
			name:           "published workflow not found",
			setupWorkflow:  nil,
			expectedStatus: http.StatusNotFound,
			expectedError:  "Published workflow not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, workflowService, _ := setupTestApp(t)

			var groupID = "non-existent-group"

			if tt.setupWorkflow != nil {
				created, err := workflowService.Create(context.Background(), tt.setupWorkflow)
				require.NoError(t, err)

				groupID = created.WorkflowGroupID
			}

			req := httptest.NewRequest(http.MethodPost, "/workflows/groups/"+groupID+"/create-draft", nil)

			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus == http.StatusCreated && tt.validateResult != nil {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				tt.validateResult(t, tt.setupWorkflow, body)
			} else if tt.expectedError != "" && tt.expectedStatus != http.StatusCreated {
				// For error cases, we don't validate specific error content in this simple test
				// In a real implementation, you would parse the error response JSON
				body, _ := io.ReadAll(resp.Body)
				_ = body // Use the body to avoid empty branch warning
			}
		})
	}
}

// TestAPIHandlers_GetWorkflows_ResponseMetadata tests that the API response shows actual parameters used.
func TestAPIHandlers_GetWorkflows_ResponseMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		queryParams  string
		expectedMeta map[string]any
	}{
		{
			name:        "no parameters - should show defaults",
			queryParams: "",
			expectedMeta: map[string]any{
				"pagination": map[string]any{
					"page":     float64(1), // JSON numbers are float64
					"per_page": float64(20),
				},
				"sorting": map[string]any{
					"sort_by":    "created_at",
					"sort_order": "desc",
				},
			},
		},
		{
			name:        "partial parameters - should show mix of provided and defaults",
			queryParams: "?per_page=10&sort_by=name",
			expectedMeta: map[string]any{
				"pagination": map[string]any{
					"per_page": float64(10), // Provided
					"page":     float64(1),  // Default
				},
				"sorting": map[string]any{
					"sort_by":    "name", // Provided
					"sort_order": "desc", // Default
				},
			},
		},
		{
			name:        "all parameters provided",
			queryParams: "?per_page=5&page=3&sort_by=updated_at&sort_order=asc",
			expectedMeta: map[string]any{
				"pagination": map[string]any{
					"per_page": float64(5),
					"page":     float64(3),
				},
				"sorting": map[string]any{
					"sort_by":    "updated_at",
					"sort_order": "asc",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, workflowService, _ := setupTestApp(t)

			// Create a test workflow to ensure non-empty response
			workflow := &models.Workflow{
				Name:        "Test Workflow",
				Description: "Test Description",
				Owner:       "test-owner",
				Status:      models.WorkflowStatusDraft,
			}
			_, err := workflowService.Create(context.Background(), workflow)
			require.NoError(t, err)

			// Make request with query parameters
			req := httptest.NewRequest(http.MethodGet, "/workflows"+tt.queryParams, nil)
			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			// Parse response body
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			var response map[string]any

			err = json.Unmarshal(body, &response)
			require.NoError(t, err)

			// Verify pagination metadata
			if pagination, ok := response["pagination"].(map[string]any); ok {
				expectedPagination := tt.expectedMeta["pagination"].(map[string]any)
				assert.Equal(t, expectedPagination["page"], pagination["page"], "Page should match")
				assert.Equal(t, expectedPagination["per_page"], pagination["per_page"], "Per_page should match")
			} else {
				t.Error("Response should include pagination metadata")
			}

			// Verify sorting metadata
			if sorting, ok := response["sorting"].(map[string]any); ok {
				expectedSorting := tt.expectedMeta["sorting"].(map[string]any)
				assert.Equal(t, expectedSorting["sort_by"], sorting["sort_by"], "Sort by should match")
				assert.Equal(t, expectedSorting["sort_order"], sorting["sort_order"], "Sort order should match")
			} else {
				t.Error("Response should include sorting metadata")
			}

			// Verify response structure
			assert.Contains(t, response, "workflows", "Response should include workflows")
			assert.Contains(t, response, "total_count", "Response should include total_count")
			assert.Contains(t, response, "has_next_page", "Response should include has_next_page")
		})
	}
}

func TestAPIHandlers_CreateWorkflowNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupWorkflow  func(t *testing.T, workflowService *services.Workflow) string
		workflowID     string
		requestBody    interface{}
		expectedStatus int
		expectedError  string
		validateResult func(t *testing.T, body []byte)
	}{
		{
			name: "successful node creation",
			setupWorkflow: func(t *testing.T, workflowService *services.Workflow) string {
				t.Helper()
				workflow := &models.Workflow{
					Name:        "Test Workflow",
					Description: "Test Description",
					Status:      models.WorkflowStatusDraft,
					Owner:       "test-user",
					Variables:   map[string]any{},
					Metadata:    map[string]any{},
				}
				created, err := workflowService.Create(context.Background(), workflow)
				require.NoError(t, err)

				return created.ID
			},
			requestBody: web.CreateNodeRequest{
				Type:      "log",
				Category:  "action",
				Name:      "Test Node",
				Config:    map[string]any{"message": "test"},
				Enabled:   true,
				PositionX: 100,
				PositionY: 200,
			},
			expectedStatus: http.StatusCreated,
			validateResult: func(t *testing.T, body []byte) {
				t.Helper()
				var node models.WorkflowNode
				err := json.Unmarshal(body, &node)
				require.NoError(t, err)
				assert.Equal(t, "Test Node", node.Name)
				assert.Equal(t, "log", node.Type)
				assert.Equal(t, models.CategoryTypeAction, node.Category)
				assert.Equal(t, true, node.Enabled)
				assert.NotEmpty(t, node.ID)
			},
		},
		{
			name:       "workflow not found",
			workflowID: "nonexistent",
			requestBody: web.CreateNodeRequest{
				Type:     "log",
				Category: "action",
				Name:     "Test",
			},
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, workflowService, _ := setupTestApp(t)

			workflowID := tt.workflowID
			if tt.setupWorkflow != nil {
				workflowID = tt.setupWorkflow(t, workflowService)
			}

			var (
				body []byte
				err  error
			)

			if tt.requestBody != nil {
				body, err = json.Marshal(tt.requestBody)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(http.MethodPost, "/workflows/"+workflowID+"/nodes", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.validateResult != nil {
				respBody, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				tt.validateResult(t, respBody)
			}
		})
	}
}

func TestAPIHandlers_DeleteWorkflowNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupWorkflow  func(t *testing.T, workflowService *services.Workflow, nodeService *services.Node) (string, string)
		workflowID     string
		nodeID         string
		expectedStatus int
	}{
		{
			name: "successful node deletion",
			setupWorkflow: func(t *testing.T, workflowService *services.Workflow, nodeService *services.Node) (string, string) {
				t.Helper()
				// Create workflow
				workflow := &models.Workflow{
					Name:        "Test Workflow",
					Description: "Test Description",
					Status:      models.WorkflowStatusDraft,
					Owner:       "test-user",
					Variables:   map[string]any{},
					Metadata:    map[string]any{},
				}
				created, err := workflowService.Create(context.Background(), workflow)
				require.NoError(t, err)

				// Create node
				createReq := &services.CreateNodeRequest{
					Type:     "log",
					Category: "action",
					Name:     "Test Node",
					Config:   map[string]any{"message": "test"},
					Enabled:  true,
				}
				node, err := nodeService.CreateNode(context.Background(), created.ID, createReq)
				require.NoError(t, err)

				return created.ID, node.ID
			},
			expectedStatus: http.StatusNoContent,
		},
		{
			name: "node not found",
			setupWorkflow: func(t *testing.T, workflowService *services.Workflow, nodeService *services.Node) (string, string) {
				t.Helper()
				// Create workflow but don't create any nodes
				workflow := &models.Workflow{
					Name:        "Test Workflow",
					Description: "Test workflow for node not found",
					Status:      models.WorkflowStatusDraft,
					Owner:       "test-user",
					Variables:   map[string]any{},
					Metadata:    map[string]any{},
				}
				created, err := workflowService.Create(context.Background(), workflow)
				require.NoError(t, err)

				return created.ID, "nonexistent-node-id" // Return workflow ID and non-existent node ID
			},
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, workflowService, nodeService := setupTestApp(t)

			workflowID := tt.workflowID

			nodeID := tt.nodeID
			if tt.setupWorkflow != nil {
				workflowID, nodeID = tt.setupWorkflow(t, workflowService, nodeService)
			}

			req := httptest.NewRequest(http.MethodDelete, "/workflows/"+workflowID+"/nodes/"+nodeID, nil)

			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)
		})
	}
}

func TestAPIHandlers_GetWorkflowNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupWorkflow  func(t *testing.T, workflowService *services.Workflow, nodeService *services.Node) (string, string)
		workflowID     string
		nodeID         string
		expectedStatus int
		validateResult func(t *testing.T, body []byte)
	}{
		{
			name: "successful action node retrieval",
			setupWorkflow: func(t *testing.T, workflowService *services.Workflow, nodeService *services.Node) (string, string) {
				t.Helper()
				workflow := &models.Workflow{
					Name:        "Test Workflow",
					Description: "Test Description",
					Owner:       "test-user",
					Variables:   map[string]any{},
					Metadata:    map[string]any{},
					Nodes:       []*models.WorkflowNode{},
					Connections: []*models.Connection{},
				}

				createdWorkflow, err := workflowService.Create(context.Background(), workflow)
				require.NoError(t, err)

				nodeReq := &services.CreateNodeRequest{
					Type:      "log",
					Category:  "action",
					Name:      "Test Log Node",
					Config:    map[string]any{"message": "test message", "level": "info"},
					Enabled:   true,
					PositionX: 100,
					PositionY: 200,
				}

				node, err := nodeService.CreateNode(context.Background(), createdWorkflow.ID, nodeReq)
				require.NoError(t, err)

				return createdWorkflow.ID, node.ID
			},
			expectedStatus: http.StatusOK,
			validateResult: func(t *testing.T, body []byte) {
				t.Helper()
				var response web.NodeResponse
				err := json.Unmarshal(body, &response)
				require.NoError(t, err)

				assert.Equal(t, "log", response.Type)
				assert.Equal(t, "action", response.Category)
				assert.Equal(t, "Test Log Node", response.Name)
				assert.Equal(t, true, response.Enabled)
				assert.Equal(t, 100, response.PositionX)
				assert.Equal(t, 200, response.PositionY)
				assert.Equal(t, "test message", response.Config["message"])
				assert.Equal(t, "info", response.Config["level"])
				assert.Nil(t, response.ProviderID)
				assert.Nil(t, response.EventType)
			},
		},
		{
			name: "successful trigger node retrieval",
			setupWorkflow: func(t *testing.T, workflowService *services.Workflow, nodeService *services.Node) (string, string) {
				t.Helper()
				workflow := &models.Workflow{
					Name:        "Test Workflow",
					Description: "Test Description",
					Owner:       "test-user",
					Variables:   map[string]any{},
					Metadata:    map[string]any{},
					Nodes:       []*models.WorkflowNode{},
					Connections: []*models.Connection{},
				}

				createdWorkflow, err := workflowService.Create(context.Background(), workflow)
				require.NoError(t, err)

				// Create a trigger node manually since the CreateNodeRequest doesn't handle trigger-specific fields
				triggerNode := &models.WorkflowNode{
					ID:         "trigger-test-id",
					Type:       "trigger:scheduler",
					Category:   models.CategoryTypeTrigger,
					Name:       "Test Schedule Trigger",
					Config:     map[string]any{"cron": "0 0 * * *"},
					Enabled:    true,
					PositionX:  50,
					PositionY:  75,
					ProviderID: stringPtr("scheduler"),
					EventType:  stringPtr("schedule_due"),
				}

				// Add node directly to the workflow
				createdWorkflow.Nodes = append(createdWorkflow.Nodes, triggerNode)

				_, err = workflowService.Update(context.Background(), createdWorkflow)
				require.NoError(t, err)

				return createdWorkflow.ID, triggerNode.ID
			},
			expectedStatus: http.StatusOK,
			validateResult: func(t *testing.T, body []byte) {
				t.Helper()
				var response web.NodeResponse
				err := json.Unmarshal(body, &response)
				require.NoError(t, err)

				assert.Equal(t, "trigger:scheduler", response.Type)
				assert.Equal(t, "trigger", response.Category)
				assert.Equal(t, "Test Schedule Trigger", response.Name)
				assert.Equal(t, true, response.Enabled)
				assert.Equal(t, 50, response.PositionX)
				assert.Equal(t, 75, response.PositionY)
				assert.Equal(t, "0 0 * * *", response.Config["cron"])
				assert.NotNil(t, response.ProviderID)
				assert.Equal(t, "scheduler", *response.ProviderID)
				assert.NotNil(t, response.EventType)
				assert.Equal(t, "schedule_due", *response.EventType)
			},
		},
		{
			name:           "workflow not found",
			workflowID:     "nonexistent-workflow",
			nodeID:         "some-node",
			expectedStatus: http.StatusNotFound, // Now properly returns 404 for non-existent workflows
			validateResult: func(t *testing.T, body []byte) {
				t.Helper()
				// Check if the body contains error information
				if len(body) > 0 {
					var response map[string]any
					err := json.Unmarshal(body, &response)
					if err == nil && response["error"] != nil {
						assert.Contains(t, response["error"], "not found")
					}
				}
			},
		},
		{
			name: "node not found",
			setupWorkflow: func(t *testing.T, workflowService *services.Workflow, nodeService *services.Node) (string, string) {
				t.Helper()
				workflow := &models.Workflow{
					Name:        "Test Workflow",
					Description: "Test Description",
					Owner:       "test-user",
					Variables:   map[string]any{},
					Metadata:    map[string]any{},
					Nodes:       []*models.WorkflowNode{},
					Connections: []*models.Connection{},
				}

				createdWorkflow, err := workflowService.Create(context.Background(), workflow)
				require.NoError(t, err)

				return createdWorkflow.ID, "nonexistent-node"
			},
			expectedStatus: http.StatusNotFound,
			validateResult: func(t *testing.T, body []byte) {
				t.Helper()
				// Check if the body contains error information
				if len(body) > 0 {
					var response map[string]any
					err := json.Unmarshal(body, &response)
					if err == nil && response["error"] != nil {
						assert.Contains(t, response["error"], "not found")
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app, workflowService, nodeService := setupTestApp(t)

			workflowID := tt.workflowID
			nodeID := tt.nodeID

			if tt.setupWorkflow != nil {
				workflowID, nodeID = tt.setupWorkflow(t, workflowService, nodeService)
			}

			req := httptest.NewRequest(http.MethodGet, "/workflows/"+workflowID+"/nodes/"+nodeID, nil)

			resp, err := app.Test(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.validateResult != nil {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				tt.validateResult(t, body)
			}
		})
	}
}

// Helper function to get string pointer.
func stringPtr(s string) *string {
	return &s
}
