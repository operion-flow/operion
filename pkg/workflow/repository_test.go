package workflow

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRepository(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

	assert.NotNil(t, repo)
	assert.Equal(t, persistence, repo.persistence)
}

func TestRepository_Create(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

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
			},
		},
		Connections: []*models.Connection{},
	}

	// Create workflow
	created, err := repo.Create(t.Context(), workflow)
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

func TestRepository_FetchByID(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

	// Create test workflow
	workflow := &models.Workflow{
		ID:          "fetch-test-workflow",
		Name:        "Fetch Test Workflow",
		Description: "Test workflow for fetching",
		Status:      models.WorkflowStatusPublished,
	}

	// Save workflow
	workflowCreated, err := repo.Create(t.Context(), workflow)
	require.NoError(t, err)

	// Fetch workflow
	fetched, err := repo.FetchByID(t.Context(), workflowCreated.ID)
	require.NoError(t, err)
	require.NotNil(t, fetched)

	// Verify workflow data
	assert.Equal(t, workflowCreated.ID, fetched.ID)
	assert.Equal(t, "Fetch Test Workflow", fetched.Name)
	assert.Equal(t, "Test workflow for fetching", fetched.Description)

	// Clean up
	err = repo.Delete(t.Context(), workflowCreated.ID)
	assert.NoError(t, err)
}

func TestRepository_FetchByID_NotFound(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

	// Try to fetch non-existent workflow
	workflow, err := repo.FetchByID(t.Context(), "non-existent")
	assert.Error(t, err)
	assert.Nil(t, workflow)
	assert.Contains(t, err.Error(), "workflow not found")
}

func TestRepository_FetchAll(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

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
		_, err := repo.Create(t.Context(), workflow)
		require.NoError(t, err)
	}

	// Fetch all workflows
	fetchedWorkflows, err := repo.FetchAll(t.Context(), "")
	require.NoError(t, err)
	assert.Len(t, fetchedWorkflows, 2)

	// Verify workflows were fetched
	workflowNames := make([]string, len(fetchedWorkflows))
	for i, workflow := range fetchedWorkflows {
		workflowNames[i] = workflow.Name
	}

	assert.Contains(t, workflowNames, "First Workflow")
	assert.Contains(t, workflowNames, "Second Workflow")

	// Clean up
	for _, workflow := range workflows {
		err = repo.Delete(t.Context(), workflow.ID)
		assert.NoError(t, err)
	}
}

func TestRepository_Update(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

	// Create initial workflow
	originalWorkflow := &models.Workflow{
		Name:        "Original Workflow",
		Description: "Original description",
		Status:      models.WorkflowStatusDraft,
	}

	workflowCreated, err := repo.Create(t.Context(), originalWorkflow)
	require.NoError(t, err)

	// Update workflow
	updatedWorkflow := &models.Workflow{
		Name:        "Updated Workflow",
		Description: "Updated description",
		Status:      models.WorkflowStatusPublished,
	}

	result, err := repo.Update(t.Context(), workflowCreated.ID, updatedWorkflow)
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
	err = repo.Delete(t.Context(), workflowCreated.ID)
	assert.NoError(t, err)
}

func TestRepository_Update_NotFound(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

	updatedWorkflow := &models.Workflow{
		Name: "Updated Workflow",
	}

	// Try to update non-existent workflow
	result, err := repo.Update(t.Context(), "non-existent", updatedWorkflow)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "workflow not found")
}

func TestRepository_Delete(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

	// Create test workflow
	workflow := &models.Workflow{
		Name: "Delete Test Workflow",
	}

	workflowCreated, err := repo.Create(t.Context(), workflow)
	require.NoError(t, err)

	// Verify workflow exists
	fetched, err := repo.FetchByID(t.Context(), workflowCreated.ID)
	require.NoError(t, err)
	assert.NotNil(t, fetched)

	// Delete workflow
	err = repo.Delete(t.Context(), workflowCreated.ID)
	require.NoError(t, err)

	// Verify workflow was deleted
	fetched, err = repo.FetchByID(t.Context(), workflowCreated.ID)
	assert.Error(t, err)
	assert.Nil(t, fetched)
}

func TestRepository_Delete_NotFound(t *testing.T) {
	persistence := createTestPersistence()
	repo := NewRepository(persistence)

	// Try to delete non-existent workflow
	err := repo.Delete(t.Context(), "non-existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflow not found")
}
