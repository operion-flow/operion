package postgresql_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/api"
	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobsResumeMakesTheWaitingJobDueWithTheNewData(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	_, execution := startJob(ctx, t, p, time.Now().Add(-time.Second))

	_, err := p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
		return []models.Job{deferredJob(execution.ID, time.Now().Add(time.Hour))}, nil
	})
	require.NoError(t, err)

	require.ErrorIs(t, p.Jobs().Resume(ctx, execution.ID, greetNodeID, nil), persistence.ErrNodeNotWaiting)
	require.NoError(t, p.Jobs().Resume(ctx, execution.ID, holdNodeID, nil))
	require.NoError(t, p.Jobs().Resume(ctx, execution.ID, holdNodeID, map[string]any{"decision": "approved"}))

	var claimed models.Job

	processed, err := p.Jobs().ProcessNext(ctx, func(_ context.Context, job models.Job, _ *models.ExecutionContext) ([]models.Job, error) {
		claimed = job

		return nil, nil
	})
	require.NoError(t, err)
	assert.True(t, processed)
	assert.Equal(t, holdNodeID, claimed.NodeID)
	assert.Equal(t, engine.ResumePort, claimed.Port)
	assert.Equal(t, map[string]any{"decision": "approved"}, claimed.Data)

	require.ErrorIs(t, p.Jobs().Resume(ctx, execution.ID, holdNodeID, nil), persistence.ErrNodeNotWaiting)
}

func TestJobsResumeKeepsTheHeldDataWithoutABody(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	_, execution := startJob(ctx, t, p, time.Now().Add(-time.Second))

	_, err := p.Jobs().ProcessNext(ctx, func(context.Context, models.Job, *models.ExecutionContext) ([]models.Job, error) {
		return []models.Job{deferredJob(execution.ID, time.Now().Add(time.Hour))}, nil
	})
	require.NoError(t, err)
	require.NoError(t, p.Jobs().Resume(ctx, execution.ID, holdNodeID, nil))

	var claimed models.Job

	_, err = p.Jobs().ProcessNext(ctx, func(_ context.Context, job models.Job, _ *models.ExecutionContext) ([]models.Job, error) {
		claimed = job

		return nil, nil
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"held": true}, claimed.Data)
}

func TestResumeContinuesAWaitingNodeWithThePostedData(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	executionID := startWaitingExecution(ctx, t, p, "{{.node_results.hold.decision}} for {{.node_results.trigger.body.employee}}")

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()

	app := fiber.New()
	api.New(workflow.NewRepository(p), workflow.NewPublishingService(p, engine.New(reg), p.EventDefinitions()), p.ExecutionContextRepository(), p.EventDefinitions(), p.Jobs(), p.RemoteNodeTypes()).Register(app)

	status, body := request(t, app, http.MethodPost, "/executions/"+executionID+"/nodes/greet/resume", `{}`)
	require.Equal(t, http.StatusConflict, status, body)

	status, body = request(t, app, http.MethodPost, "/executions/"+executionID+"/nodes/hold/resume", `{"decision": "approved"}`)
	require.Equal(t, http.StatusAccepted, status, body)

	saved := waitForStatus(ctx, t, p, executionID, models.ExecutionStatusCompleted)
	assert.Equal(t, "approved for Ana", saved.NodeResults[greetNodeID].Data[messageKey])
	assert.Equal(t, "approved", saved.NodeResults[holdNodeID].Data["decision"])
	assert.Empty(t, pendingJobs(ctx, t, databaseURL, executionID))

	status, body = request(t, app, http.MethodPost, "/executions/"+executionID+"/nodes/hold/resume", `{}`)
	assert.Equal(t, http.StatusConflict, status, body)
}
