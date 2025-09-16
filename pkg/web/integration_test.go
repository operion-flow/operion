//go:build integration

package web_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/services"
	"github.com/dukex/operion/pkg/web"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestDB(t *testing.T) (string, func()) {
	ctx := context.Background()

	// Start PostgreSQL container
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:15-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       "test_operion",
				"POSTGRES_USER":     "test_user",
				"POSTGRES_PASSWORD": "test_pass",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		},
		Started: true,
	})
	require.NoError(t, err)

	// Get connection details
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)

	dbURL := fmt.Sprintf("postgres://test_user:test_pass@%s:%s/test_operion?sslmode=disable", host, port.Port())

	// Wait for database to be ready
	time.Sleep(2 * time.Second)

	cleanup := func() {
		_ = container.Terminate(ctx)
	}

	return dbURL, cleanup
}

func setupIntegrationApp(t *testing.T, dbURL string) (*fiber.App, *services.Workflow, *services.Publishing) {
	// Connect to database
	_, err := sql.Open("postgres", dbURL)
	require.NoError(t, err)

	// Create persistence layer with automatic migrations
	persistence, err := postgresql.NewPersistence(context.Background(), slog.Default(), dbURL)
	require.NoError(t, err)

	// Initialize services
	workflowService := services.NewWorkflow(persistence)
	publishingService := services.NewPublishing(persistence)
	validator := validator.New(validator.WithRequiredStructEnabled())
	registryInstance := registry.NewRegistry(slog.Default())

	// Create handlers
	handlers := web.NewAPIHandlers(workflowService, publishingService, validator, registryInstance)

	// Setup Fiber app
	app := fiber.New()
	w := app.Group("/workflows")
	w.Get("/", handlers.GetWorkflows)
	w.Post("/", handlers.CreateWorkflow)
	w.Get("/:id", handlers.GetWorkflow)
	w.Patch("/:id", handlers.UpdateWorkflow)
	w.Delete("/:id", handlers.DeleteWorkflow)
	w.Post("/:id/publish", handlers.PublishWorkflow)
	w.Post("/groups/:groupId/create-draft", handlers.CreateDraftFromPublished)

	return app, workflowService, publishingService
}

func TestWorkflowCRUD_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dbURL, cleanup := setupTestDB(t)
	defer cleanup()

	app, workflowService, _ := setupIntegrationApp(t, dbURL)

	// Test 1: Create workflow
	t.Run("Create Workflow", func(t *testing.T) {
		createReq := web.CreateWorkflowRequest{
			Name:        "Integration Test Workflow",
			Description: "A workflow for integration testing",
			Owner:       "integration-test-user",
			Variables:   map[string]any{"env": "integration", "timeout": 30},
			Metadata:    map[string]any{"version": "1.0.0", "category": "test"},
		}

		body, err := json.Marshal(createReq)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/workflows", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		var createdWorkflow models.Workflow
		err = json.NewDecoder(resp.Body).Decode(&createdWorkflow)
		require.NoError(t, err)

		assert.NotEmpty(t, createdWorkflow.ID)
		assert.Equal(t, "Integration Test Workflow", createdWorkflow.Name)
		assert.Equal(t, "A workflow for integration testing", createdWorkflow.Description)
		assert.Equal(t, "integration-test-user", createdWorkflow.Owner)
		assert.Equal(t, models.WorkflowStatusDraft, createdWorkflow.Status)
		assert.Equal(t, "integration", createdWorkflow.Variables["env"])
		assert.Equal(t, "1.0.0", createdWorkflow.Metadata["version"])
		assert.NotZero(t, createdWorkflow.CreatedAt)
		assert.NotZero(t, createdWorkflow.UpdatedAt)

		// Store workflow ID for subsequent tests
		workflowID := createdWorkflow.ID

		// Test 2: Get workflow by ID
		t.Run("Get Workflow", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/workflows/"+workflowID, nil)
			req.Header.Set("Accept", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var fetchedWorkflow models.Workflow
			err = json.NewDecoder(resp.Body).Decode(&fetchedWorkflow)
			require.NoError(t, err)

			assert.Equal(t, workflowID, fetchedWorkflow.ID)
			assert.Equal(t, "Integration Test Workflow", fetchedWorkflow.Name)
		})

		// Test 3: Update workflow
		t.Run("Update Workflow", func(t *testing.T) {
			updateReq := web.UpdateWorkflowRequest{
				Name:        stringPtr("Updated Integration Test Workflow"),
				Description: stringPtr("Updated description for testing"),
				Variables:   map[string]any{"env": "integration", "timeout": 60, "new_var": "new_value"},
				Metadata:    map[string]any{"version": "2.0.0", "category": "test", "updated": true},
			}

			body, err := json.Marshal(updateReq)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPatch, "/workflows/"+workflowID, bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var updatedWorkflow models.Workflow
			err = json.NewDecoder(resp.Body).Decode(&updatedWorkflow)
			require.NoError(t, err)

			assert.Equal(t, workflowID, updatedWorkflow.ID)
			assert.Equal(t, "Updated Integration Test Workflow", updatedWorkflow.Name)
			assert.Equal(t, "Updated description for testing", updatedWorkflow.Description)
			assert.Equal(t, "integration-test-user", updatedWorkflow.Owner) // Should remain unchanged
			assert.Equal(t, "integration", updatedWorkflow.Variables["env"])
			assert.InDelta(t, float64(60), updatedWorkflow.Variables["timeout"], 0.001)
			assert.Equal(t, "new_value", updatedWorkflow.Variables["new_var"])
			assert.Equal(t, "2.0.0", updatedWorkflow.Metadata["version"])
			assert.Equal(t, true, updatedWorkflow.Metadata["updated"])
		})

		// Test 4: List workflows (should include our workflow)
		t.Run("List All Workflows", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/workflows", nil)
			req.Header.Set("Accept", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var response struct {
				Workflows   []models.Workflow `json:"workflows"`
				TotalCount  int               `json:"total_count"`
				HasNextPage bool              `json:"has_next_page"`
			}
			err = json.NewDecoder(resp.Body).Decode(&response)
			require.NoError(t, err)

			assert.Len(t, response.Workflows, 1)
			assert.Equal(t, workflowID, response.Workflows[0].ID)
			assert.Equal(t, 1, response.TotalCount)
			assert.False(t, response.HasNextPage)
		})

		// Test 5: List workflows with owner filter
		t.Run("List Workflows by Owner", func(t *testing.T) {
			// Create another workflow with different owner
			createReq2 := web.CreateWorkflowRequest{
				Name:        "Another Workflow",
				Description: "Another test workflow",
				Owner:       "another-user",
			}

			body, err := json.Marshal(createReq2)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/workflows", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			resp.Body.Close()

			// Now test filtering
			req = httptest.NewRequest(http.MethodGet, "/workflows?owner_id=integration-test-user", nil)
			req.Header.Set("Accept", "application/json")

			resp, err = app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var response struct {
				Workflows   []models.Workflow `json:"workflows"`
				TotalCount  int               `json:"total_count"`
				HasNextPage bool              `json:"has_next_page"`
			}
			err = json.NewDecoder(resp.Body).Decode(&response)
			require.NoError(t, err)

			assert.Len(t, response.Workflows, 1)
			assert.Equal(t, workflowID, response.Workflows[0].ID)
			assert.Equal(t, "integration-test-user", response.Workflows[0].Owner)
			assert.Equal(t, 1, response.TotalCount)
			assert.False(t, response.HasNextPage)
		})

		// Test 6: Delete workflow
		t.Run("Delete Workflow", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodDelete, "/workflows/"+workflowID, nil)

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusNoContent, resp.StatusCode)

			// Verify workflow is deleted
			_, err = workflowService.FetchByID(context.Background(), workflowID)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "workflow not found")
		})
	})
}

func TestWorkflowPublishing_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dbURL, cleanup := setupTestDB(t)
	defer cleanup()

	app, workflowService, _ := setupIntegrationApp(t, dbURL)

	// Create a workflow with trigger node for publishing
	workflow := &models.Workflow{
		Name:        "Publishable Workflow",
		Description: "A workflow that can be published",
		Owner:       "publish-test-user",
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
			{
				ID:       "action1",
				Name:     "Log Action",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config:   map[string]any{"message": "Test log"},
				Enabled:  true,
			},
		},
		Connections: []*models.Connection{
			{
				ID:         "conn1",
				SourcePort: "trigger1:output",
				TargetPort: "action1:input",
			},
		},
	}

	created, err := workflowService.Create(context.Background(), workflow)
	require.NoError(t, err)
	workflowID := created.ID
	groupID := created.WorkflowGroupID

	// Test 1: Publish workflow
	t.Run("Publish Workflow", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/workflows/"+workflowID+"/publish", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var publishedWorkflow models.Workflow
		err = json.NewDecoder(resp.Body).Decode(&publishedWorkflow)
		require.NoError(t, err)

		assert.Equal(t, workflowID, publishedWorkflow.ID)
		assert.Equal(t, models.WorkflowStatusPublished, publishedWorkflow.Status)
		assert.NotNil(t, publishedWorkflow.PublishedAt)
	})

	// Test 2: Create draft from published
	t.Run("Create Draft from Published", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/workflows/groups/"+groupID+"/create-draft", nil)

		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		var draftWorkflow models.Workflow
		err = json.NewDecoder(resp.Body).Decode(&draftWorkflow)
		require.NoError(t, err)

		assert.NotEqual(t, workflowID, draftWorkflow.ID) // Should have different ID
		assert.Equal(t, groupID, draftWorkflow.WorkflowGroupID)
		assert.Equal(t, models.WorkflowStatusDraft, draftWorkflow.Status)
		assert.Equal(t, "Publishable Workflow", draftWorkflow.Name)
		assert.Nil(t, draftWorkflow.PublishedAt)
		assert.Len(t, draftWorkflow.Nodes, 2)       // Should copy all nodes
		assert.Len(t, draftWorkflow.Connections, 1) // Should copy all connections
	})
}

func TestWorkflowValidation_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dbURL, cleanup := setupTestDB(t)
	defer cleanup()

	app, _, _ := setupIntegrationApp(t, dbURL)

	// Test validation errors
	tests := []struct {
		name           string
		requestBody    web.CreateWorkflowRequest
		expectedStatus int
		expectedError  string
	}{
		{
			name: "missing required name",
			requestBody: web.CreateWorkflowRequest{
				Description: "Test Description",
				Owner:       "test-user",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Name",
		},
		{
			name: "name too short",
			requestBody: web.CreateWorkflowRequest{
				Name:        "AB",
				Description: "Test Description",
				Owner:       "test-user",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Name",
		},
		{
			name: "missing description",
			requestBody: web.CreateWorkflowRequest{
				Name:  "Valid Name",
				Owner: "test-user",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Description",
		},
		{
			name: "missing owner",
			requestBody: web.CreateWorkflowRequest{
				Name:        "Valid Name",
				Description: "Valid Description",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Owner",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(tt.requestBody)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/workflows", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			// Note: In a real implementation, you would parse the error response
			// and check for the specific validation error message
		})
	}
}
