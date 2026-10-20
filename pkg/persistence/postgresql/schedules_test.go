package postgresql_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const scheduleNodeID = "schedule"

func saveSchedulerWorkflow(ctx context.Context, t *testing.T, p *postgresql.Persistence, status models.WorkflowStatus) string {
	t.Helper()

	id := uuid.NewString()
	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              id,
		Name:            "daily digest",
		Description:     "runs every morning",
		Status:          status,
		WorkflowGroupID: id,
		Nodes: []*models.WorkflowNode{
			{ID: scheduleNodeID, Name: scheduleNodeID, Type: models.NodeTypeTriggerScheduler, Category: models.CategoryTypeTrigger, Enabled: true, Config: map[string]any{"cron_expression": "0 9 * * *", "timezone": "America/Sao_Paulo"}},
		},
	}))

	return id
}

func nextRunAt(ctx context.Context, t *testing.T, databaseURL, workflowID string) (time.Time, bool) {
	t.Helper()

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	var at time.Time

	err = db.QueryRowContext(ctx, `SELECT next_run_at FROM schedules WHERE workflow_id = $1`, workflowID).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false
	}

	require.NoError(t, err)

	return at, true
}

func firing(schedule models.Schedule, nextRun time.Time) models.Firing {
	execution := &models.ExecutionContext{
		ID:          uuid.NewString(),
		WorkflowID:  schedule.WorkflowID,
		Status:      models.ExecutionStatusRunning,
		NodeResults: map[string]models.NodeResult{},
		TriggerData: map[string]any{"scheduled_time": schedule.NextRunAt.Format(time.RFC3339)},
		CreatedAt:   time.Now().UTC(),
	}

	return models.Firing{
		Execution: execution,
		Job: models.Job{
			ID:          uuid.NewString(),
			ExecutionID: execution.ID,
			NodeID:      schedule.NodeID,
			Port:        "external",
			Data:        execution.TriggerData,
			RunAt:       time.Now().UTC(),
		},
		NextRunAt: nextRun,
	}
}

func TestSchedulesSyncCreatesRowsForPublishedSchedulerTriggers(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	published := saveSchedulerWorkflow(ctx, t, p, models.WorkflowStatusPublished)
	draft := saveSchedulerWorkflow(ctx, t, p, models.WorkflowStatusDraft)
	saveWebhookWorkflow(ctx, t, p, "/ignored", models.WorkflowStatusPublished)

	first := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

	var seen []models.Schedule

	require.NoError(t, p.Schedules().Sync(ctx, func(schedule models.Schedule) (time.Time, error) {
		seen = append(seen, schedule)

		return first, nil
	}))

	require.Len(t, seen, 1)
	assert.Equal(t, models.Schedule{WorkflowID: published, NodeID: scheduleNodeID, Expression: "0 9 * * *", Timezone: "America/Sao_Paulo"}, seen[0])

	at, ok := nextRunAt(ctx, t, databaseURL, published)
	require.True(t, ok)
	assert.True(t, first.Equal(at))

	_, ok = nextRunAt(ctx, t, databaseURL, draft)
	assert.False(t, ok)

	require.NoError(t, p.Schedules().Sync(ctx, func(models.Schedule) (time.Time, error) {
		t.Error("an existing schedule must not be computed again")

		return first, nil
	}))
}

func TestSchedulesSyncReportsScheduleErrors(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	workflowID := saveSchedulerWorkflow(ctx, t, p, models.WorkflowStatusPublished)
	errCron := errors.New("bad cron")

	err := p.Schedules().Sync(ctx, func(models.Schedule) (time.Time, error) { return time.Time{}, errCron })

	require.ErrorIs(t, err, errCron)

	_, ok := nextRunAt(ctx, t, databaseURL, workflowID)
	assert.False(t, ok)
}

func TestSchedulesProcessDueStartsExecutionAndMovesSchedule(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	workflowID := saveSchedulerWorkflow(ctx, t, p, models.WorkflowStatusPublished)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	later := time.Now().Add(time.Hour).UTC().Truncate(time.Second)

	require.NoError(t, p.Schedules().Sync(ctx, func(models.Schedule) (time.Time, error) { return due, nil }))

	var started models.Firing

	processed, err := p.Schedules().ProcessDue(ctx, func(_ context.Context, schedule models.Schedule) (models.Firing, error) {
		assert.Equal(t, workflowID, schedule.WorkflowID)
		assert.Equal(t, "0 9 * * *", schedule.Expression)
		assert.True(t, due.Equal(schedule.NextRunAt))

		started = firing(schedule, later)

		return started, nil
	})

	require.NoError(t, err)
	assert.True(t, processed)

	execution, err := p.ExecutionContextRepository().GetExecutionContext(ctx, started.Execution.ID)
	require.NoError(t, err)
	assert.Equal(t, workflowID, execution.WorkflowID)

	at, _ := nextRunAt(ctx, t, databaseURL, workflowID)
	assert.True(t, later.Equal(at))

	claimed, err := p.Jobs().ProcessNext(ctx, func(_ context.Context, job models.Job, _ *models.ExecutionContext) ([]models.Job, error) {
		assert.Equal(t, started.Job.ID, job.ID)

		return nil, nil
	})
	require.NoError(t, err)
	assert.True(t, claimed)

	processed, err = p.Schedules().ProcessDue(ctx, func(context.Context, models.Schedule) (models.Firing, error) {
		t.Error("the schedule moved to the future")

		return models.Firing{}, nil
	})
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestSchedulesProcessDueKeepsScheduleWhenFireFails(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	workflowID := saveSchedulerWorkflow(ctx, t, p, models.WorkflowStatusPublished)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	errFire := errors.New("fire failed")

	require.NoError(t, p.Schedules().Sync(ctx, func(models.Schedule) (time.Time, error) { return due, nil }))

	processed, err := p.Schedules().ProcessDue(ctx, func(context.Context, models.Schedule) (models.Firing, error) {
		return models.Firing{}, errFire
	})

	require.ErrorIs(t, err, errFire)
	assert.True(t, processed)

	at, _ := nextRunAt(ctx, t, databaseURL, workflowID)
	assert.True(t, due.Equal(at))
}

func TestSchedulesProcessDueSkipsUnpublishedWorkflows(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	draft := saveSchedulerWorkflow(ctx, t, p, models.WorkflowStatusDraft)

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(ctx, `INSERT INTO schedules (workflow_id, node_id, next_run_at) VALUES ($1, $2, NOW() - INTERVAL '1 minute')`, draft, scheduleNodeID)
	require.NoError(t, err)

	processed, err := p.Schedules().ProcessDue(ctx, func(context.Context, models.Schedule) (models.Firing, error) {
		t.Error("draft workflows do not fire")

		return models.Firing{}, nil
	})

	require.NoError(t, err)
	assert.False(t, processed)
}
