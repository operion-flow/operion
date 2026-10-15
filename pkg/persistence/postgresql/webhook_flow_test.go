package postgresql_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/worker"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func saveWebhookWorkflow(ctx context.Context, t *testing.T, p *postgresql.Persistence, path string, status models.WorkflowStatus) string {
	t.Helper()

	id := uuid.NewString()
	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              id,
		Name:            "employee created",
		Description:     "greets new employees",
		Status:          status,
		WorkflowGroupID: id,
		Variables:       map[string]any{"team": "Vendas"},
		Nodes: []*models.WorkflowNode{
			{ID: triggerNodeID, Name: triggerNodeID, Type: webhookNodeType, Category: models.CategoryTypeTrigger, Enabled: true, Config: map[string]any{"webhook_path": path}},
			{ID: greetNodeID, Name: greetNodeID, Type: logNodeType, Category: models.CategoryTypeAction, Enabled: true, Config: map[string]any{messageKey: "hello {{.node_results.trigger.body.employee}} from {{.variables.team}}"}},
		},
		Connections: []*models.Connection{{ID: "trigger-greet", SourcePort: "trigger:success", TargetPort: "greet:main"}},
	}))

	return id
}

func TestWebhookTriggersMatchPublishedWorkflowsByPath(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	published := saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusPublished)
	saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusDraft)
	saveWebhookWorkflow(ctx, t, p, "/other", models.WorkflowStatusPublished)

	matches, err := p.WebhookTriggers(ctx, "/employees")

	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, published, matches[0].WorkflowID)
	assert.Equal(t, triggerNodeID, matches[0].TriggerNode.ID)
}

func TestWebhookExecutionRunsThroughJobsTable(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	logger := slog.New(slog.DiscardHandler)

	saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusPublished)

	app := fiber.New()
	ingest.NewWebhook(p, p.Jobs(), logger).Register(app)

	resp, err := app.Test(httptest.NewRequestWithContext(ctx, http.MethodPost, "/webhooks/employees", strings.NewReader(`{"employee": "Ana"}`)))
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	var accepted struct {
		Executions []string `json:"executions"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&accepted))
	require.Len(t, accepted.Executions, 1)

	reg := registry.NewRegistry(logger)
	reg.RegisterDefaultNodes()

	workerCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		_ = worker.New(p.Jobs(), p.WorkflowRepository(), engine.New(reg), logger, 10*time.Millisecond).Run(workerCtx)
	}()

	var execution *models.ExecutionContext

	require.Eventually(t, func() bool {
		execution, err = p.ExecutionContextRepository().GetExecutionContext(ctx, accepted.Executions[0])

		return err == nil && execution.Status != models.ExecutionStatusRunning
	}, 10*time.Second, 20*time.Millisecond)

	assert.Equal(t, models.ExecutionStatusCompleted, execution.Status, execution.ErrorMessage)
	assert.Equal(t, "hello Ana from Vendas", execution.NodeResults[greetNodeID].Data[messageKey])
}
