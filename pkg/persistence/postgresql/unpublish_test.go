package postgresql_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/api"
	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func inAMinute(models.Schedule) (time.Time, error) {
	return time.Now().Add(time.Minute), nil
}

func missTheNextRun(ctx context.Context, t *testing.T, databaseURL, workflowID string) {
	t.Helper()

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(ctx, `UPDATE schedules SET next_run_at = NOW() - INTERVAL '1 hour' WHERE workflow_id = $1`, workflowID)
	require.NoError(t, err)
}

func noScheduleFires(ctx context.Context, t *testing.T, p *postgresql.Persistence) {
	t.Helper()

	processed, err := p.Schedules().ProcessDue(ctx, func(context.Context, models.Schedule) (models.Firing, error) {
		t.Error("a schedule fired")

		return models.Firing{}, nil
	})

	require.NoError(t, err)
	assert.False(t, processed)
}

func TestUnpublishStopsTheWorkflowsTriggers(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	webhook := saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusPublished)
	kafka := saveKafkaWorkflow(ctx, t, p, "employees", models.WorkflowStatusPublished)

	require.NoError(t, p.WorkflowRepository().UnpublishWorkflow(ctx, webhook))
	require.NoError(t, p.WorkflowRepository().UnpublishWorkflow(ctx, kafka))

	assert.Equal(t, models.WorkflowStatusUnpublished, statusOf(ctx, t, p, webhook))
	assert.Equal(t, models.WorkflowStatusUnpublished, statusOf(ctx, t, p, kafka))

	matches, err := p.WebhookTriggers(ctx, acmeTenant, "/employees")
	require.NoError(t, err)
	assert.Empty(t, matches)

	topics, err := p.KafkaTopics(ctx)
	require.NoError(t, err)
	assert.Empty(t, topics)
}

func TestUnpublishOnlyTakesAPublishedWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	draft := saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusDraft)

	require.ErrorIs(t, p.WorkflowRepository().UnpublishWorkflow(ctx, draft), persistence.ErrWorkflowNotPublished)
	assert.Equal(t, models.WorkflowStatusDraft, statusOf(ctx, t, p, draft))

	require.ErrorIs(t, p.WorkflowRepository().UnpublishWorkflow(ctx, uuid.NewString()), persistence.ErrWorkflowNotPublished)
}

func TestUnpublishDropsTheWorkflowsSchedules(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	id := saveSchedulerWorkflow(ctx, t, p, models.WorkflowStatusPublished)
	require.NoError(t, p.Schedules().Sync(ctx, inAMinute))
	missTheNextRun(ctx, t, databaseURL, id)

	require.NoError(t, p.WorkflowRepository().UnpublishWorkflow(ctx, id))

	_, scheduled := nextRunAt(ctx, t, databaseURL, id)
	assert.False(t, scheduled)

	require.NoError(t, p.WorkflowRepository().PublishWorkflow(ctx, id))
	require.NoError(t, p.Schedules().Sync(ctx, inAMinute))

	noScheduleFires(ctx, t, p)
}

func TestRepublishingAnOlderVersionDoesNotFireTheRunsItMissed(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	first := saveSchedulerWorkflow(ctx, t, p, models.WorkflowStatusPublished)
	require.NoError(t, p.Schedules().Sync(ctx, inAMinute))
	missTheNextRun(ctx, t, databaseURL, first)

	second := uuid.NewString()
	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              second,
		Name:            "daily digest",
		Description:     "runs every morning",
		Status:          models.WorkflowStatusDraft,
		WorkflowGroupID: first,
		Nodes: []*models.WorkflowNode{
			{ID: scheduleNodeID, Name: scheduleNodeID, Type: models.NodeTypeTriggerScheduler, Category: models.CategoryTypeTrigger, Config: map[string]any{"cron_expression": dailyAtNine}},
		},
	}))
	require.NoError(t, p.WorkflowRepository().PublishWorkflow(ctx, second))
	require.NoError(t, p.WorkflowRepository().PublishWorkflow(ctx, first))
	require.NoError(t, p.Schedules().Sync(ctx, inAMinute))

	noScheduleFires(ctx, t, p)
}

func TestAWaitingExecutionFinishesAfterItsWorkflowIsUnpublished(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	executionID := startWaitingExecution(ctx, t, p, "welcome {{.node_results.trigger.body.employee}}")

	execution, err := p.ExecutionContextRepository().GetExecutionContext(ctx, executionID)
	require.NoError(t, err)
	require.NoError(t, p.WorkflowRepository().UnpublishWorkflow(ctx, execution.WorkflowID))

	makeJobsDue(ctx, t, databaseURL, executionID)

	saved := waitForStatus(ctx, t, p, executionID, models.ExecutionStatusCompleted)
	assert.Equal(t, "welcome Ana", saved.NodeResults[greetNodeID].Data[messageKey])
}

func TestUnpublishThroughAPI(t *testing.T) {
	p, _, _ := setupTestDB(t)

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()

	app := fiber.New()
	api.New(workflow.NewRepository(p), workflow.NewPublishingService(p, engine.New(reg), p.EventDefinitions()), p.ExecutionContextRepository(), p.EventDefinitions(), p.Jobs(), p.RemoteNodeTypes()).Register(app, apiToken)
	ingest.NewWebhook(p, p.Jobs(), slog.New(slog.DiscardHandler)).Register(app)

	status, created := request(t, app, http.MethodPost, "/workflows", `{
		"name": "Employee Criado",
		"description": "greets new employees",
		"nodes": [
			{"id": "trigger", "name": "trigger", "type": "trigger:webhook", "category": "trigger", "enabled": false, "config": {"webhook_path": "/unpublish"}},
			{"id": "greet", "name": "greet", "type": "log", "category": "action", "config": {"message": "hello"}}
		],
		"connections": [{"id": "trigger-greet", "source_port": "trigger:success", "target_port": "greet:main"}]
	}`)
	require.Equal(t, http.StatusCreated, status)

	id := created["id"].(string)

	status, _ = request(t, app, http.MethodPost, "/workflows/"+id+"/publish", "")
	require.Equal(t, http.StatusOK, status)

	status, _ = request(t, app, http.MethodPost, "/webhooks/"+acmeTenant+"/unpublish", `{}`)
	require.Equal(t, http.StatusAccepted, status)

	status, unpublished := request(t, app, http.MethodPost, "/workflows/"+id+"/unpublish", "")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, string(models.WorkflowStatusUnpublished), unpublished["status"])

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhooks/"+acmeTenant+"/unpublish", strings.NewReader(`{}`))
	resp, err := app.Test(req)
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, string(body))

	status, problem := request(t, app, http.MethodPost, "/workflows/"+id+"/unpublish", "")
	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, problem["detail"], "workflow is not published")
}
