package persistence_test

import (
	"errors"
	"testing"

	"github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/assert"
)

func TestStandardizedErrors(t *testing.T) {
	t.Parallel()

	t.Run("error constants are available", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, persistence.ErrWorkflowNotFound)
		assert.NotNil(t, persistence.ErrPublishedWorkflowNotFound)
		assert.NotNil(t, persistence.ErrDraftWorkflowNotFound)
		assert.NotNil(t, persistence.ErrNodeNotFound)
		assert.NotNil(t, persistence.ErrConnectionNotFound)
	})

	t.Run("error checking functions work correctly", func(t *testing.T) {
		t.Parallel()

		workflowErr := persistence.NewWorkflowError("GetByID", "workflow-123", persistence.ErrWorkflowNotFound)
		groupErr := persistence.NewWorkflowGroupError("CreateDraft", "group-456", persistence.ErrPublishedWorkflowNotFound)

		assert.True(t, persistence.IsWorkflowNotFound(workflowErr))
		assert.True(t, persistence.IsPublishedWorkflowNotFound(groupErr))

		// Test error unwrapping
		assert.True(t, errors.Is(workflowErr, persistence.ErrWorkflowNotFound))
		assert.True(t, errors.Is(groupErr, persistence.ErrPublishedWorkflowNotFound))
	})

	t.Run("workflow error contains context", func(t *testing.T) {
		t.Parallel()

		err := persistence.NewWorkflowError("UpdateWorkflow", "workflow-123", persistence.ErrWorkflowNotFound)

		assert.Contains(t, err.Error(), "UpdateWorkflow")
		assert.Contains(t, err.Error(), "workflow-123")
		assert.Contains(t, err.Error(), "workflow not found")
	})

	t.Run("workflow group error contains context", func(t *testing.T) {
		t.Parallel()

		err := persistence.NewWorkflowGroupError("CreateDraftFromPublished", "group-456", persistence.ErrPublishedWorkflowNotFound)

		assert.Contains(t, err.Error(), "CreateDraftFromPublished")
		assert.Contains(t, err.Error(), "group-456")
		assert.Contains(t, err.Error(), "published workflow not found")
	})
}
