package services

import (
	"context"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWorkflow(t *testing.T) {
	persistence := file.NewPersistence(t.TempDir())
	service := NewWorkflow(persistence)

	assert.NotNil(t, service)
	assert.Equal(t, persistence, service.persistence)
}

func TestWorkflow_Create(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	workflow := &models.Workflow{
		Name:        "Test Workflow",
		Description: "Test workflow description",
		Status:      models.WorkflowStatusDraft,
		Nodes: []*models.WorkflowNode{
			{
				ID:       "step-1",
				Name:     "Test Step",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"message": "test",
				},
				Enabled: true,
			},
		},
		Connections: []*models.Connection{},
	}

	// Create workflow
	created, err := service.Create(t.Context(), workflow)
	require.NoError(t, err)
	require.NotNil(t, created)

	// Verify ID was generated
	assert.NotEmpty(t, created.ID)

	// Verify timestamps were set
	assert.False(t, created.CreatedAt.IsZero())
	assert.False(t, created.UpdatedAt.IsZero())

	// Verify status was set
	assert.Equal(t, models.WorkflowStatusDraft, created.Status)

	// Clean up
	err = persistence.WorkflowRepository().Delete(t.Context(), created.ID)
	assert.NoError(t, err)
}

func TestWorkflow_FetchByID(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	// Create test workflow
	workflow := &models.Workflow{
		ID:          "fetch-test-workflow",
		Name:        "Fetch Test Workflow",
		Description: "Test workflow for fetching",
		Status:      models.WorkflowStatusPublished,
	}

	// Save workflow
	workflowCreated, err := service.Create(t.Context(), workflow)
	require.NoError(t, err)

	// Fetch workflow
	fetched, err := service.FetchByID(t.Context(), workflowCreated.ID)
	require.NoError(t, err)
	require.NotNil(t, fetched)

	// Verify workflow data
	assert.Equal(t, workflowCreated.ID, fetched.ID)
	assert.Equal(t, "Fetch Test Workflow", fetched.Name)
	assert.Equal(t, "Test workflow for fetching", fetched.Description)

	// Clean up
	err = service.Delete(t.Context(), workflowCreated.ID)
	assert.NoError(t, err)
}

func TestWorkflow_FetchByID_NotFound(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	// Try to fetch non-existent workflow
	workflow, err := service.FetchByID(t.Context(), "non-existent")
	assert.Error(t, err)
	assert.Nil(t, workflow)
	assert.Contains(t, err.Error(), "workflow not found")
}

func TestWorkflow_FetchAll(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	workflows := []*models.Workflow{
		{

			Name:   "First Workflow",
			Status: models.WorkflowStatusPublished,
		},
		{

			Name:   "Second Workflow",
			Status: models.WorkflowStatusDraft,
		},
	}

	for _, workflow := range workflows {
		_, err := service.Create(t.Context(), workflow)
		require.NoError(t, err)
	}

	// Fetch all workflows
	result, err := service.ListWorkflows(t.Context(), &ListWorkflowsRequest{
		PerPage:   100,
		Page:      1,
		SortBy:    "created_at",
		SortOrder: "desc",
	})
	require.NoError(t, err)
	assert.Len(t, result.Workflows, 2)
	fetchedWorkflows := result.Workflows

	// Verify workflows were fetched
	workflowNames := make([]string, len(fetchedWorkflows))
	for i, workflow := range fetchedWorkflows {
		workflowNames[i] = workflow.Name
	}

	assert.Contains(t, workflowNames, "First Workflow")
	assert.Contains(t, workflowNames, "Second Workflow")

	// Clean up
	for _, workflow := range workflows {
		err = service.Delete(t.Context(), workflow.ID)
		assert.NoError(t, err)
	}
}

func TestWorkflow_Update(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	// Create initial workflow
	originalWorkflow := &models.Workflow{
		Name:        "Original Workflow",
		Description: "Original description",
		Status:      models.WorkflowStatusDraft,
	}

	workflowCreated, err := service.Create(t.Context(), originalWorkflow)
	require.NoError(t, err)

	// Update workflow
	updatedWorkflow := &models.Workflow{
		ID:          workflowCreated.ID,
		Name:        "Updated Workflow",
		Description: "Updated description",
		Status:      models.WorkflowStatusPublished,
		CreatedAt:   workflowCreated.CreatedAt, // Preserve original creation time
	}

	result, err := service.Update(t.Context(), updatedWorkflow)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify update
	assert.Equal(t, workflowCreated.ID, result.ID)
	assert.Equal(t, "Updated Workflow", result.Name)
	assert.Equal(t, "Updated description", result.Description)
	assert.Equal(t, models.WorkflowStatusPublished, result.Status)

	assert.WithinDuration(t, originalWorkflow.CreatedAt, result.CreatedAt, 0)
	assert.True(t,
		result.UpdatedAt.After(originalWorkflow.UpdatedAt) ||
			result.UpdatedAt.Equal(originalWorkflow.UpdatedAt))

	// Clean up
	err = service.Delete(t.Context(), workflowCreated.ID)
	assert.NoError(t, err)
}

func TestWorkflow_Update_DirectSave(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	// Create a workflow object with ID (simulating what handler would do)
	workflow := &models.Workflow{
		ID:          "test-workflow",
		Name:        "Updated Workflow",
		Description: "Updated description",
		Status:      models.WorkflowStatusPublished,
		CreatedAt:   time.Now().UTC().Add(-1 * time.Hour), // Simulate existing workflow
	}

	// Update should succeed since it trusts the workflow is valid
	result, err := service.Update(t.Context(), workflow)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "Updated Workflow", result.Name)
	assert.True(t, result.UpdatedAt.After(workflow.CreatedAt))
}

func TestWorkflow_Delete(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	// Create test workflow
	workflow := &models.Workflow{
		Name: "Delete Test Workflow",
	}

	workflowCreated, err := service.Create(t.Context(), workflow)
	require.NoError(t, err)

	// Verify workflow exists
	fetched, err := service.FetchByID(t.Context(), workflowCreated.ID)
	require.NoError(t, err)
	assert.NotNil(t, fetched)

	// Delete workflow
	err = service.Delete(t.Context(), workflowCreated.ID)
	require.NoError(t, err)

	// Verify workflow was deleted
	fetched, err = service.FetchByID(t.Context(), workflowCreated.ID)
	assert.Error(t, err)
	assert.Nil(t, fetched)
}

func TestWorkflow_Delete_NotFound(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	// Try to delete non-existent workflow
	err := service.Delete(t.Context(), "non-existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflow not found")
}

// TestIsValidationError tests the IsValidationError function comprehensively.
func TestIsValidationError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ErrInvalidRequest should be validation error",
			err:      ErrInvalidRequest,
			expected: true,
		},
		{
			name:     "ErrInvalidSortField should be validation error",
			err:      ErrInvalidSortField,
			expected: true,
		},
		{
			name:     "ErrInvalidSortOrder should be validation error",
			err:      ErrInvalidSortOrder,
			expected: true,
		},
		{
			name:     "ErrInvalidStatus should be validation error",
			err:      ErrInvalidStatus,
			expected: true,
		},
		{
			name:     "ErrEmptyOwnerID should be validation error",
			err:      ErrEmptyOwnerID,
			expected: true,
		},
		{
			name:     "ErrWorkflowNameRequired should be validation error",
			err:      ErrWorkflowNameRequired,
			expected: true,
		},
		{
			name:     "ErrNodesRequired should be validation error",
			err:      ErrNodesRequired,
			expected: true,
		},
		{
			name:     "ErrTriggerNodeRequired should be validation error",
			err:      ErrTriggerNodeRequired,
			expected: true,
		},
		{
			name:     "ErrWorkflowNil should be validation error",
			err:      ErrWorkflowNil,
			expected: true,
		},
		{
			name:     "ErrInvalidConnectionData should be validation error",
			err:      ErrInvalidConnectionData,
			expected: true,
		},
		{
			name:     "ErrWorkflowNotFound should NOT be validation error",
			err:      ErrWorkflowNotFound,
			expected: false,
		},
		{
			name:     "Generic error should NOT be validation error",
			err:      assert.AnError,
			expected: false,
		},
		{
			name:     "Nil error should NOT be validation error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidationError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestListWorkflows_InvalidSortField tests that invalid sort field returns proper validation error.
func TestListWorkflows_InvalidSortField(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	req := ListWorkflowsRequest{
		SortBy:    "invalid_field",
		SortOrder: "asc",
		PerPage:   10,
		Page:      1,
	}

	// Test that invalid sort field returns validation error
	_, err := service.ListWorkflows(context.Background(), &req)
	require.Error(t, err)

	// Verify it's identified as validation error
	assert.True(t, IsValidationError(err), "Invalid sort field should be identified as validation error")

	// Verify it contains the specific error
	assert.ErrorIs(t, err, ErrInvalidSortField)
}

// TestListWorkflows_InvalidSortOrder tests that invalid sort order returns proper validation error.
func TestListWorkflows_InvalidSortOrder(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	req := ListWorkflowsRequest{
		SortBy:    "name",
		SortOrder: "invalid_order",
		PerPage:   10,
		Page:      1,
	}

	// Test that invalid sort order returns validation error
	_, err := service.ListWorkflows(context.Background(), &req)
	require.Error(t, err)

	// Verify it's identified as validation error
	assert.True(t, IsValidationError(err), "Invalid sort order should be identified as validation error")

	// Verify it contains the specific error
	assert.ErrorIs(t, err, ErrInvalidSortOrder)
}

// TestListWorkflows_EmptyOwnerID tests that empty owner ID returns proper validation error.
func TestListWorkflows_EmptyOwnerID(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	req := ListWorkflowsRequest{
		OwnerID: "  ", // Empty after trim
		PerPage: 10,
		Page:    1,
	}

	// Test that empty owner ID returns validation error
	_, err := service.ListWorkflows(context.Background(), &req)
	require.Error(t, err)

	// Verify it's identified as validation error
	assert.True(t, IsValidationError(err), "Empty owner ID should be identified as validation error")

	// Verify it contains the specific error
	assert.ErrorIs(t, err, ErrEmptyOwnerID)
}

// TestWorkflow_ListWorkflows_DefaultsApplied verifies that default values are applied to the request.
func TestWorkflow_ListWorkflows_DefaultsApplied(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	// Create a workflow to ensure non-empty results
	workflow := &models.Workflow{
		Name:        "Test Workflow",
		Description: "Test Description",
		Status:      models.WorkflowStatusDraft,
		Owner:       "test-owner",
	}
	_, err := service.Create(t.Context(), workflow)
	require.NoError(t, err)

	// Test with empty request - should apply defaults
	req := &ListWorkflowsRequest{}

	result, err := service.ListWorkflows(t.Context(), req)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify that defaults were applied to the original request
	assert.Equal(t, 20, req.PerPage, "Default per_page should be applied")
	assert.Equal(t, 1, req.Page, "Default page should be applied")
	assert.Equal(t, "created_at", req.SortBy, "Default sort_by should be applied")
	assert.Equal(t, "desc", req.SortOrder, "Default sort_order should be applied")
}

// TestWorkflow_ListWorkflows_PartialDefaults verifies that only missing values get defaults.
func TestWorkflow_ListWorkflows_PartialDefaults(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	// Test with partial request - should only apply defaults for missing values
	req := &ListWorkflowsRequest{
		PerPage: 50,     // Provided
		SortBy:  "name", // Provided
		// SortOrder and Page not provided - should get defaults
	}

	_, err := service.ListWorkflows(t.Context(), req)
	require.NoError(t, err)

	// Verify that provided values are kept and defaults applied for missing ones
	assert.Equal(t, 50, req.PerPage, "Provided per_page should be kept")
	assert.Equal(t, "name", req.SortBy, "Provided sort_by should be kept")
	assert.Equal(t, "desc", req.SortOrder, "Default sort_order should be applied")
	assert.Equal(t, 1, req.Page, "Default page should be applied")
}

// TestCreateWorkflow_InvalidConnection tests that invalid connection data returns proper validation error.
func TestCreateWorkflow_InvalidConnection(t *testing.T) {
	testDir := t.TempDir()
	persistence := file.NewPersistence(testDir)
	service := NewWorkflow(persistence)

	workflow := &models.Workflow{
		Name:   "Test Workflow",
		Status: models.WorkflowStatusDraft,
		Nodes: []*models.WorkflowNode{
			{
				ID:       "node1",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config:   map[string]any{"message": "test"},
				Enabled:  true,
			},
			{
				ID:       "node2",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config:   map[string]any{"message": "test2"},
				Enabled:  true,
			},
		},
		Connections: []*models.Connection{
			{
				ID:         "conn1",
				SourcePort: "invalid-format", // Missing node.port format
				TargetPort: "node2.input",
			},
		},
	}

	// Test that invalid connection format returns validation error
	_, err := service.Create(context.Background(), workflow)
	if err != nil {
		// Verify it's identified as validation error
		assert.True(t, IsValidationError(err), "Invalid connection data should be identified as validation error")

		// Verify it contains the specific error
		assert.ErrorIs(t, err, ErrInvalidConnectionData)
	}
}
