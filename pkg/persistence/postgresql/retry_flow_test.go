package postgresql_test

import (
	"context"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobsKeepTheAttemptOfQueuedJobs(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	_, execution := startJob(ctx, t, p, time.Now().Add(-time.Second))

	retry := deferredJob(execution.ID, time.Now().Add(time.Hour))
	retry.Attempt = 2

	_, err := p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
		return []models.Job{retry}, nil
	})
	require.NoError(t, err)

	makeJobsDue(ctx, t, databaseURL, execution.ID)

	var claimed models.Job

	_, err = p.Jobs().ProcessNext(ctx, func(_ context.Context, job models.Job, _ *models.ExecutionContext) ([]models.Job, error) {
		claimed = job

		return nil, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, claimed.Attempt)
}

func TestFailedNodeIsRetriedFromAJobAndFailsWhenTheAttemptsRunOut(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	policy := &models.RetryPolicy{Attempts: 2, Initial: "1h", Max: "2h"}

	executionID := startFlow(ctx, t, p, &models.WorkflowNode{ID: greetNodeID, Type: "transform", Config: map[string]any{"expression": "{not json}"}, Retry: policy})
	saved := waitForNodeStatus(ctx, t, p, executionID, greetNodeID, models.NodeStatusRetrying)

	stored, err := p.WorkflowRepository().GetByID(ctx, saved.WorkflowID)
	require.NoError(t, err)
	assert.Nil(t, stored.Nodes[0].Retry)
	assert.Equal(t, policy, stored.Nodes[1].Retry)

	runAts := pendingJobs(ctx, t, databaseURL, executionID)
	require.Len(t, runAts, 1)
	assert.WithinDuration(t, time.Now().Add(time.Hour), runAts[0], time.Minute)
	assert.Equal(t, models.ExecutionStatusRunning, saved.Status)
	assert.NotEmpty(t, saved.NodeResults[greetNodeID].Error)

	makeJobsDue(ctx, t, databaseURL, executionID)

	saved = waitForStatus(ctx, t, p, executionID, models.ExecutionStatusFailed)
	assert.Contains(t, saved.ErrorMessage, `node "greet"`)
	assert.Empty(t, pendingJobs(ctx, t, databaseURL, executionID))
}
