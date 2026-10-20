package postgresql_test

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pendingJobs(ctx context.Context, t *testing.T, databaseURL, executionID string) []time.Time {
	t.Helper()

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `SELECT run_at FROM jobs WHERE execution_id = $1 ORDER BY run_at`, executionID)
	require.NoError(t, err)

	defer func() { _ = rows.Close() }()

	var runAts []time.Time

	for rows.Next() {
		var runAt time.Time
		require.NoError(t, rows.Scan(&runAt))

		runAts = append(runAts, runAt)
	}

	require.NoError(t, rows.Err())

	return runAts
}

func makeJobsDue(ctx context.Context, t *testing.T, databaseURL, executionID string) {
	t.Helper()

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(ctx, `UPDATE jobs SET run_at = NOW() - INTERVAL '1 second' WHERE execution_id = $1`, executionID)
	require.NoError(t, err)
}

func deferredJob(executionID string, runAt time.Time) models.Job {
	return models.Job{ID: uuid.NewString(), ExecutionID: executionID, NodeID: holdNodeID, Port: engine.ResumePort, Data: map[string]any{"held": true}, RunAt: runAt}
}

func finishBranch(status models.ExecutionStatus, queued ...models.Job) func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
	return func(_ context.Context, _ models.Job, loaded *models.ExecutionContext) ([]models.Job, error) {
		now := time.Now().UTC()
		loaded.Status = status
		loaded.CompletedAt = &now

		return queued, nil
	}
}

func TestJobsProcessNextCompletesOnlyAfterTheLastQueuedJob(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	_, execution := startJob(ctx, t, p, time.Now().Add(-time.Second))
	later := time.Now().Add(time.Hour).UTC().Truncate(time.Second)

	processed, err := p.Jobs().ProcessNext(ctx, finishBranch(models.ExecutionStatusCompleted, deferredJob(execution.ID, later)))
	require.NoError(t, err)
	assert.True(t, processed)

	saved, err := p.ExecutionContextRepository().GetExecutionContext(ctx, execution.ID)
	require.NoError(t, err)
	assert.Equal(t, models.ExecutionStatusRunning, saved.Status)
	assert.Nil(t, saved.CompletedAt)

	runAts := pendingJobs(ctx, t, databaseURL, execution.ID)
	require.Len(t, runAts, 1)
	assert.True(t, later.Equal(runAts[0]))

	processed, err = p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
		t.Error("the queued job is not due yet")

		return nil, nil
	})
	require.NoError(t, err)
	assert.False(t, processed)

	makeJobsDue(ctx, t, databaseURL, execution.ID)

	processed, err = p.Jobs().ProcessNext(ctx, finishBranch(models.ExecutionStatusCompleted))
	require.NoError(t, err)
	assert.True(t, processed)

	saved, err = p.ExecutionContextRepository().GetExecutionContext(ctx, execution.ID)
	require.NoError(t, err)
	assert.Equal(t, models.ExecutionStatusCompleted, saved.Status)
	assert.NotNil(t, saved.CompletedAt)
	assert.Empty(t, pendingJobs(ctx, t, databaseURL, execution.ID))
}

func TestJobsProcessNextDropsQueuedJobsOfAFailedExecution(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	_, execution := startJob(ctx, t, p, time.Now().Add(-time.Second))

	due := deferredJob(execution.ID, time.Now().Add(-time.Second))
	future := deferredJob(execution.ID, time.Now().Add(time.Hour))

	_, err := p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
		return []models.Job{due, future}, nil
	})
	require.NoError(t, err)
	require.Len(t, pendingJobs(ctx, t, databaseURL, execution.ID), 2)

	processed, err := p.Jobs().ProcessNext(ctx, finishBranch(models.ExecutionStatusFailed))
	require.NoError(t, err)
	assert.True(t, processed)

	saved, err := p.ExecutionContextRepository().GetExecutionContext(ctx, execution.ID)
	require.NoError(t, err)
	assert.Equal(t, models.ExecutionStatusFailed, saved.Status)
	assert.Empty(t, pendingJobs(ctx, t, databaseURL, execution.ID))
}

func TestWaitNodeHoldsTheExecutionUntilItsJobIsDue(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	logger := slog.New(slog.DiscardHandler)

	workflowID := uuid.NewString()
	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              workflowID,
		Name:            "welcome later",
		Description:     "waits before greeting",
		Status:          models.WorkflowStatusPublished,
		WorkflowGroupID: workflowID,
		Nodes: []*models.WorkflowNode{
			{ID: triggerNodeID, Name: triggerNodeID, Type: webhookNodeType, Category: models.CategoryTypeTrigger, Enabled: true, Config: map[string]any{"webhook_path": "/later"}},
			{ID: holdNodeID, Name: holdNodeID, Type: "wait", Category: models.CategoryTypeAction, Enabled: true, Config: map[string]any{"duration": "1h"}},
			{ID: greetNodeID, Name: greetNodeID, Type: logNodeType, Category: models.CategoryTypeAction, Enabled: true, Config: map[string]any{messageKey: "welcome {{.node_results.trigger.body.employee}}"}},
		},
		Connections: []*models.Connection{
			{ID: "trigger-hold", SourcePort: "trigger:success", TargetPort: "hold:main"},
			{ID: "hold-greet", SourcePort: "hold:success", TargetPort: "greet:main"},
		},
	}))

	data := map[string]any{bodyKey: map[string]any{"employee": "Ana"}}
	activation := engine.TriggerActivation(triggerNodeID, data)
	execution := &models.ExecutionContext{ID: uuid.NewString(), WorkflowID: workflowID, Status: models.ExecutionStatusRunning, NodeResults: map[string]models.NodeResult{}, TriggerData: data, CreatedAt: time.Now().UTC()}
	require.NoError(t, p.Jobs().Start(ctx, execution, models.Job{ID: uuid.NewString(), ExecutionID: execution.ID, NodeID: activation.NodeID, Port: activation.Port, Data: activation.Data, RunAt: time.Now().UTC()}))

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()

	workerCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		_ = worker.New(p.Jobs(), p.WorkflowRepository(), engine.New(reg), logger, 10*time.Millisecond).Run(workerCtx)
	}()

	require.Eventually(t, func() bool {
		saved, err := p.ExecutionContextRepository().GetExecutionContext(ctx, execution.ID)

		return err == nil && saved.NodeResults[holdNodeID].Status == string(models.NodeStatusWaiting)
	}, 10*time.Second, 20*time.Millisecond)

	runAts := pendingJobs(ctx, t, databaseURL, execution.ID)
	require.Len(t, runAts, 1)
	assert.WithinDuration(t, time.Now().Add(time.Hour), runAts[0], time.Minute)

	saved, err := p.ExecutionContextRepository().GetExecutionContext(ctx, execution.ID)
	require.NoError(t, err)
	assert.Equal(t, models.ExecutionStatusRunning, saved.Status)
	assert.NotContains(t, saved.NodeResults, greetNodeID)

	makeJobsDue(ctx, t, databaseURL, execution.ID)

	require.Eventually(t, func() bool {
		saved, err = p.ExecutionContextRepository().GetExecutionContext(ctx, execution.ID)

		return err == nil && saved.Status == models.ExecutionStatusCompleted
	}, 10*time.Second, 20*time.Millisecond)

	assert.Equal(t, "welcome Ana", saved.NodeResults[greetNodeID].Data[messageKey])
	assert.Empty(t, pendingJobs(ctx, t, databaseURL, execution.ID))
}
