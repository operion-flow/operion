package postgresql_test

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/postgresql"
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

func startFlow(ctx context.Context, t *testing.T, p *postgresql.Persistence, steps ...*models.WorkflowNode) string {
	t.Helper()

	workflowID := uuid.NewString()
	wf := &models.Workflow{
		ID:              workflowID,
		Name:            "linear flow",
		Description:     "runs its steps one after another",
		Status:          models.WorkflowStatusPublished,
		WorkflowGroupID: workflowID,
		TenantID:        acmeTenant,
		Nodes: []*models.WorkflowNode{
			{ID: triggerNodeID, Name: triggerNodeID, Type: webhookNodeType, Category: models.CategoryTypeTrigger, Config: map[string]any{"webhook_path": "/flow"}},
		},
	}

	previous := triggerNodeID

	for _, step := range steps {
		step.Name, step.Category = step.ID, models.CategoryTypeAction
		wf.Nodes = append(wf.Nodes, step)
		wf.Connections = append(wf.Connections, &models.Connection{ID: previous + "-" + step.ID, SourcePort: previous + ":success", TargetPort: step.ID + ":main"})
		previous = step.ID
	}

	require.NoError(t, p.WorkflowRepository().Save(ctx, wf))

	data := map[string]any{bodyKey: map[string]any{"employee": "Ana"}}
	activation := engine.TriggerActivation(triggerNodeID, data)
	execution := &models.ExecutionContext{ID: uuid.NewString(), WorkflowID: workflowID, Status: models.ExecutionStatusRunning, NodeResults: map[string]models.NodeResult{}, TriggerData: data, CreatedAt: time.Now().UTC()}
	require.NoError(t, p.Jobs().Start(ctx, execution, models.Job{ID: uuid.NewString(), ExecutionID: execution.ID, NodeID: activation.NodeID, Port: activation.Port, Data: activation.Data, RunAt: time.Now().UTC()}))

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()

	workerCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)

	go func() {
		_ = worker.New(p.Jobs(), p.WorkflowRepository(), engine.New(reg), slog.New(slog.DiscardHandler), 10*time.Millisecond).Run(workerCtx)
	}()

	return execution.ID
}

func eventually(ctx context.Context, t *testing.T, p *postgresql.Persistence, executionID string, done func(*models.ExecutionContext) bool) *models.ExecutionContext {
	t.Helper()

	var saved *models.ExecutionContext

	require.Eventually(t, func() bool {
		var err error

		saved, err = p.ExecutionContextRepository().GetExecutionContext(ctx, executionID)

		return err == nil && done(saved)
	}, 10*time.Second, 20*time.Millisecond)

	return saved
}

func waitForStatus(ctx context.Context, t *testing.T, p *postgresql.Persistence, executionID string, status models.ExecutionStatus) *models.ExecutionContext {
	t.Helper()

	return eventually(ctx, t, p, executionID, func(saved *models.ExecutionContext) bool { return saved.Status == status })
}

func waitForNodeStatus(ctx context.Context, t *testing.T, p *postgresql.Persistence, executionID, nodeID string, status models.NodeStatus) *models.ExecutionContext {
	t.Helper()

	return eventually(ctx, t, p, executionID, func(saved *models.ExecutionContext) bool {
		return saved.NodeResults[nodeID].Status == string(status)
	})
}

func startWaitingExecution(ctx context.Context, t *testing.T, p *postgresql.Persistence, message string) string {
	t.Helper()

	executionID := startFlow(ctx, t, p,
		&models.WorkflowNode{ID: holdNodeID, Type: "wait", Config: map[string]any{"duration": "1h"}},
		&models.WorkflowNode{ID: greetNodeID, Type: logNodeType, Config: map[string]any{messageKey: message}},
	)
	waitForNodeStatus(ctx, t, p, executionID, holdNodeID, models.NodeStatusWaiting)

	return executionID
}

func TestWaitNodeHoldsTheExecutionUntilItsJobIsDue(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	executionID := startWaitingExecution(ctx, t, p, "welcome {{.node_results.trigger.body.employee}}")

	runAts := pendingJobs(ctx, t, databaseURL, executionID)
	require.Len(t, runAts, 1)
	assert.WithinDuration(t, time.Now().Add(time.Hour), runAts[0], time.Minute)

	saved, err := p.ExecutionContextRepository().GetExecutionContext(ctx, executionID)
	require.NoError(t, err)
	assert.Equal(t, models.ExecutionStatusRunning, saved.Status)
	assert.NotContains(t, saved.NodeResults, greetNodeID)

	makeJobsDue(ctx, t, databaseURL, executionID)

	saved = waitForStatus(ctx, t, p, executionID, models.ExecutionStatusCompleted)
	assert.Equal(t, "welcome Ana", saved.NodeResults[greetNodeID].Data[messageKey])
	assert.Empty(t, pendingJobs(ctx, t, databaseURL, executionID))
}
