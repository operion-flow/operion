package postgresql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	triggerNodeID   = "trigger"
	greetNodeID     = "greet"
	webhookNodeType = "trigger:webhook"
	logNodeType     = "log"
	messageKey      = "message"
	holdNodeID      = "hold"
	bodyKey         = "body"
)

func startJob(ctx context.Context, t *testing.T, p *postgresql.Persistence, runAt time.Time) (models.Job, *models.ExecutionContext) {
	t.Helper()

	workflowID := uuid.NewString()
	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              workflowID,
		Name:            "jobs workflow",
		Description:     "jobs workflow",
		Status:          models.WorkflowStatusPublished,
		WorkflowGroupID: workflowID,
	}))

	data := map[string]any{bodyKey: map[string]any{"employee": "Ana"}}
	execution := &models.ExecutionContext{
		ID:          uuid.NewString(),
		WorkflowID:  workflowID,
		Status:      models.ExecutionStatusRunning,
		NodeResults: map[string]models.NodeResult{},
		TriggerData: data,
		CreatedAt:   time.Now().UTC(),
	}
	job := models.Job{
		ID:          uuid.NewString(),
		ExecutionID: execution.ID,
		NodeID:      triggerNodeID,
		Port:        "external",
		Data:        data,
		RunAt:       runAt,
	}

	require.NoError(t, p.Jobs().Start(ctx, execution, job))

	return job, execution
}

func TestJobsProcessNextRunsDueJobAndRemovesIt(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	started, execution := startJob(ctx, t, p, time.Now().Add(-time.Second))

	processed, err := p.Jobs().ProcessNext(ctx, func(_ context.Context, job models.Job, loaded *models.ExecutionContext) ([]models.Job, error) {
		assert.Equal(t, started.ID, job.ID)
		assert.Equal(t, triggerNodeID, job.NodeID)
		assert.Equal(t, "external", job.Port)
		assert.Equal(t, "Ana", job.Data[bodyKey].(map[string]any)["employee"])
		assert.Equal(t, execution.ID, loaded.ID)
		assert.Equal(t, execution.WorkflowID, loaded.WorkflowID)

		now := time.Now().UTC()
		loaded.Status = models.ExecutionStatusCompleted
		loaded.NodeResults[greetNodeID] = models.NodeResult{NodeID: greetNodeID, Data: map[string]any{"text": "hi"}}
		loaded.CompletedAt = &now

		return nil, nil
	})

	require.NoError(t, err)
	assert.True(t, processed)

	saved, err := p.ExecutionContextRepository().GetExecutionContext(ctx, execution.ID)
	require.NoError(t, err)
	assert.Equal(t, models.ExecutionStatusCompleted, saved.Status)
	assert.Equal(t, "hi", saved.NodeResults[greetNodeID].Data["text"])
	assert.NotNil(t, saved.CompletedAt)

	processed, err = p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
		t.Fatal("no job should be left")

		return nil, nil
	})
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestJobsProcessNextKeepsJobWhenProcessFails(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	_, execution := startJob(ctx, t, p, time.Now().Add(-time.Second))
	errProcess := errors.New("process failed")

	processed, err := p.Jobs().ProcessNext(ctx, func(_ context.Context, _ models.Job, loaded *models.ExecutionContext) ([]models.Job, error) {
		loaded.Status = models.ExecutionStatusFailed

		return nil, errProcess
	})

	require.ErrorIs(t, err, errProcess)
	assert.True(t, processed)

	saved, err := p.ExecutionContextRepository().GetExecutionContext(ctx, execution.ID)
	require.NoError(t, err)
	assert.Equal(t, models.ExecutionStatusRunning, saved.Status)

	processed, err = p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
		return nil, nil
	})
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestJobsProcessNextSkipsLockedAndFutureJobs(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	startJob(ctx, t, p, time.Now().Add(-time.Second))
	startJob(ctx, t, p, time.Now().Add(time.Hour))

	claimed := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error)

	go func() {
		_, err := p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
			close(claimed)
			<-release

			return nil, nil
		})
		done <- err
	}()

	<-claimed

	processed, err := p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
		t.Error("the due job is locked and the other one is in the future")

		return nil, nil
	})
	require.NoError(t, err)
	assert.False(t, processed)

	close(release)
	require.NoError(t, <-done)
}

func TestJobsStartIgnoresARepeatedExecution(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	job, execution := startJob(ctx, t, p, time.Now().Add(-time.Second))

	repeated := job
	repeated.ID = uuid.NewString()
	require.NoError(t, p.Jobs().Start(ctx, execution, repeated))

	var claimed []string

	for {
		processed, err := p.Jobs().ProcessNext(ctx, func(_ context.Context, next models.Job, _ *models.ExecutionContext) ([]models.Job, error) {
			claimed = append(claimed, next.ID)

			return nil, nil
		})
		require.NoError(t, err)

		if !processed {
			break
		}
	}

	assert.Equal(t, []string{job.ID}, claimed)
}
