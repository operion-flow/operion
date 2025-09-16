package file

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	persistencepkg "github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPersistence(t *testing.T) {
	// Test with regular path
	persistence := NewPersistence("/tmp/test")
	fp := persistence.(*Persistence)
	assert.Equal(t, "/tmp/test", fp.root)

	// Test with file:// prefix
	persistence = NewPersistence("file:///tmp/test")
	fp = persistence.(*Persistence)
	assert.Equal(t, "/tmp/test", fp.root)
}

func TestPersistence_Close(t *testing.T) {
	persistence := NewPersistence("./test-data")
	err := persistence.Close(t.Context())
	assert.NoError(t, err)
}

func TestPersistence_SaveWorkflow(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	workflow := &models.Workflow{
		ID:          "test-workflow",
		Name:        "Test Workflow",
		Description: "Test workflow description",
		Status:      models.WorkflowStatusPublished,
		Nodes: []*models.WorkflowNode{
			{
				ID:       "node-1",
				Name:     "Test Node",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"message": "test",
				},
				Enabled: true,
			},
		},
	}

	// Save workflow
	err := persistence.WorkflowRepository().Save(t.Context(), workflow)
	require.NoError(t, err)

	// Verify file was created
	filePath := filepath.Join(testDir, "workflows", "test-workflow.json")
	assert.FileExists(t, filePath)

	// Verify timestamps were set
	assert.False(t, workflow.CreatedAt.IsZero())
	assert.False(t, workflow.UpdatedAt.IsZero())
}

func TestPersistence_SaveWorkflow_UpdatesTimestamp(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	workflow := &models.Workflow{
		ID:        "update-workflow",
		Name:      "Update Test Workflow",
		CreatedAt: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	// Save workflow
	err := persistence.WorkflowRepository().Save(t.Context(), workflow)
	require.NoError(t, err)

	// Verify CreatedAt was preserved and UpdatedAt was set
	assert.Equal(t, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), workflow.CreatedAt)
	assert.True(t, workflow.UpdatedAt.After(workflow.CreatedAt))
}

func TestPersistence_WorkflowByID(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	// Create test workflow
	originalWorkflow := &models.Workflow{
		ID:          "fetch-workflow",
		Name:        "Fetch Test Workflow",
		Description: "Test workflow for fetching",
		Status:      models.WorkflowStatusPublished,
		Nodes: []*models.WorkflowNode{
			{
				ID:       "node-1",
				Name:     "Test Node",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Config: map[string]any{
					"message": "test",
				},
				Enabled: true,
			},
		},
	}

	// Save workflow
	err := persistence.WorkflowRepository().Save(t.Context(), originalWorkflow)
	require.NoError(t, err)

	// Fetch workflow
	fetchedWorkflow, err := persistence.WorkflowRepository().GetByID(t.Context(), "fetch-workflow")
	require.NoError(t, err)
	require.NotNil(t, fetchedWorkflow)

	// Verify workflow data
	assert.Equal(t, "fetch-workflow", fetchedWorkflow.ID)
	assert.Equal(t, "Fetch Test Workflow", fetchedWorkflow.Name)
	assert.Equal(t, "Test workflow for fetching", fetchedWorkflow.Description)
	assert.Equal(t, models.WorkflowStatusPublished, fetchedWorkflow.Status)
	assert.Len(t, fetchedWorkflow.Nodes, 1)
	assert.Equal(t, "node-1", fetchedWorkflow.Nodes[0].ID)
}

func TestPersistence_WorkflowByID_NotFound(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	// Try to fetch non-existent workflow
	workflow, err := persistence.WorkflowRepository().GetByID(t.Context(), "non-existent")
	require.NoError(t, err)
	require.Nil(t, workflow)
}

func TestPersistence_Workflows(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	// Create multiple test workflows
	workflows := []*models.Workflow{
		{
			ID:     "workflow-1",
			Name:   "First Workflow",
			Status: models.WorkflowStatusPublished,
		},
		{
			ID:     "workflow-2",
			Name:   "Second Workflow",
			Status: models.WorkflowStatusDraft,
		},
		{
			ID:     "workflow-3",
			Name:   "Third Workflow",
			Status: models.WorkflowStatusPublished,
		},
	}

	// Save all workflows
	for _, workflow := range workflows {
		err := persistence.WorkflowRepository().Save(t.Context(), workflow)
		require.NoError(t, err)
	}

	// Fetch all workflows
	result, err := persistence.WorkflowRepository().ListWorkflows(t.Context(), persistencepkg.ListWorkflowsOptions{
		Limit: 100, // Get all workflows for test
	})
	require.NoError(t, err)
	require.Len(t, result.Workflows, 3)

	// Verify workflows were fetched (order might be different)
	workflowIDs := make([]string, len(result.Workflows))
	for i, workflow := range result.Workflows {
		workflowIDs[i] = workflow.ID
	}

	assert.Contains(t, workflowIDs, "workflow-1")
	assert.Contains(t, workflowIDs, "workflow-2")
	assert.Contains(t, workflowIDs, "workflow-3")
}

func TestPersistence_Workflows_EmptyDirectory(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	// Fetch workflows from empty directory
	result, err := persistence.WorkflowRepository().ListWorkflows(t.Context(), persistencepkg.ListWorkflowsOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	assert.Empty(t, result.Workflows)
}

func TestPersistence_Workflows_NoDirectory(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	// Try to fetch workflows without creating directory first
	result, err := persistence.WorkflowRepository().ListWorkflows(t.Context(), persistencepkg.ListWorkflowsOptions{
		Limit: 100,
	})
	// fs.Glob on a non-existent directory returns empty slice with no error
	assert.NoError(t, err)
	assert.Empty(t, result.Workflows)
}

func TestPersistence_DeleteWorkflow(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	// Create test workflow
	workflow := &models.Workflow{
		ID:   "delete-workflow",
		Name: "Delete Test Workflow",
	}

	// Save workflow
	err := persistence.WorkflowRepository().Save(t.Context(), workflow)
	require.NoError(t, err)

	// Verify file exists
	filePath := filepath.Join(testDir, "workflows", "delete-workflow.json")
	assert.FileExists(t, filePath)

	// Delete workflow
	err = persistence.WorkflowRepository().Delete(t.Context(), "delete-workflow")
	require.NoError(t, err)

	// Verify file was deleted
	assert.NoFileExists(t, filePath)
}

func TestPersistence_DeleteWorkflow_NotFound(t *testing.T) {
	testDir := t.TempDir()

	persistence := NewPersistence(testDir)

	// Try to delete non-existent workflow (should not error)
	err := persistence.WorkflowRepository().Delete(t.Context(), "non-existent")
	assert.NoError(t, err)
}
