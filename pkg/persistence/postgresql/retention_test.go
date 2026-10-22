package postgresql_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func saveExecution(ctx context.Context, t *testing.T, p *postgresql.Persistence, workflowID string, status models.ExecutionStatus, created time.Time, completed *time.Time) string {
	t.Helper()

	id := uuid.NewString()
	require.NoError(t, p.ExecutionContextRepository().SaveExecutionContext(ctx, &models.ExecutionContext{
		ID:          id,
		WorkflowID:  workflowID,
		Status:      status,
		NodeResults: map[string]models.NodeResult{},
		CreatedAt:   created,
		CompletedAt: completed,
	}))

	return id
}

func exists(ctx context.Context, t *testing.T, p *postgresql.Persistence, executionID string) bool {
	t.Helper()

	_, err := p.ExecutionContextRepository().GetExecutionContext(ctx, executionID)

	return err == nil
}

func countRows(ctx context.Context, t *testing.T, databaseURL, query string, args ...any) int {
	t.Helper()

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	var count int
	require.NoError(t, db.QueryRowContext(ctx, query, args...).Scan(&count))

	return count
}

func TestDeleteFinishedExecutionsKeepsWhatIsRecentOrUnfinished(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	workflowID := saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusPublished)

	now := time.Now().UTC()
	longAgo, lately := now.Add(-3*time.Hour), now.Add(-10*time.Minute)
	finishedLongAgo := now.Add(-2 * time.Hour)

	oldCompleted := saveExecution(ctx, t, p, workflowID, models.ExecutionStatusCompleted, longAgo, &finishedLongAgo)
	oldFailed := saveExecution(ctx, t, p, workflowID, models.ExecutionStatusFailed, longAgo, &finishedLongAgo)
	stillRunning := saveExecution(ctx, t, p, workflowID, models.ExecutionStatusRunning, longAgo, nil)
	finishedLately := saveExecution(ctx, t, p, workflowID, models.ExecutionStatusCompleted, longAgo, &lately)
	startedLately := saveExecution(ctx, t, p, workflowID, models.ExecutionStatusCompleted, lately, &lately)

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	for _, executionID := range []string{oldCompleted, stillRunning} {
		_, err = db.ExecContext(ctx, `INSERT INTO jobs (id, execution_id, node_id, port) VALUES ($1, $2, 'greet', 'main')`, uuid.NewString(), executionID)
		require.NoError(t, err)
	}

	deleted, err := p.DeleteFinishedExecutions(ctx, now.Add(-time.Hour), 100)

	require.NoError(t, err)
	assert.Equal(t, 2, deleted)
	assert.False(t, exists(ctx, t, p, oldCompleted))
	assert.False(t, exists(ctx, t, p, oldFailed))
	assert.True(t, exists(ctx, t, p, stillRunning))
	assert.True(t, exists(ctx, t, p, finishedLately))
	assert.True(t, exists(ctx, t, p, startedLately))
	assert.Zero(t, countRows(ctx, t, databaseURL, `SELECT COUNT(*) FROM jobs WHERE execution_id = $1`, oldCompleted))
	assert.Equal(t, 1, countRows(ctx, t, databaseURL, `SELECT COUNT(*) FROM jobs WHERE execution_id = $1`, stillRunning))
}

func TestDeleteFinishedExecutionsTakesOneBatchAtATime(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	workflowID := saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusPublished)

	longAgo := time.Now().UTC().Add(-3 * time.Hour)
	for range 3 {
		saveExecution(ctx, t, p, workflowID, models.ExecutionStatusCompleted, longAgo, &longAgo)
	}

	cutoff := time.Now().UTC().Add(-time.Hour)

	first, err := p.DeleteFinishedExecutions(ctx, cutoff, 2)
	require.NoError(t, err)

	second, err := p.DeleteFinishedExecutions(ctx, cutoff, 2)
	require.NoError(t, err)

	third, err := p.DeleteFinishedExecutions(ctx, cutoff, 2)
	require.NoError(t, err)

	assert.Equal(t, []int{2, 1, 0}, []int{first, second, third})
}

func TestReplicasDeletingAtTheSameTimeShareTheWork(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	workflowID := saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusPublished)

	const executions = 40

	longAgo := time.Now().UTC().Add(-3 * time.Hour)
	for range executions {
		saveExecution(ctx, t, p, workflowID, models.ExecutionStatusCompleted, longAgo, &longAgo)
	}

	cutoff := time.Now().UTC().Add(-time.Hour)

	var (
		replicas sync.WaitGroup
		mu       sync.Mutex
		total    int
	)

	for range 4 {
		replicas.Go(func() {
			for {
				deleted, err := p.DeleteFinishedExecutions(ctx, cutoff, 3)
				assert.NoError(t, err)

				mu.Lock()
				total += deleted
				mu.Unlock()

				if err != nil || deleted == 0 {
					return
				}
			}
		})
	}

	replicas.Wait()

	assert.Equal(t, executions, total)
	assert.Zero(t, countRows(ctx, t, databaseURL, `SELECT COUNT(*) FROM execution_contexts`))
}
