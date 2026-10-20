package postgresql_test

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/dukex/operion/pkg/nodes/notify"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/worker"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const acmeTenant = "acme"

func saveWebhookWorkflow(ctx context.Context, t *testing.T, p *postgresql.Persistence, path string, status models.WorkflowStatus) string {
	t.Helper()

	id := uuid.NewString()
	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              id,
		Name:            "employee created",
		Description:     "greets new employees",
		Status:          status,
		WorkflowGroupID: id,
		TenantID:        acmeTenant,
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

	matches, err := p.WebhookTriggers(ctx, acmeTenant, "/employees")

	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, published, matches[0].WorkflowID)
	assert.Equal(t, triggerNodeID, matches[0].TriggerNode.ID)

	otherTenant, err := p.WebhookTriggers(ctx, "globex", "/employees")
	require.NoError(t, err)
	assert.Empty(t, otherTenant)
}

func request(t *testing.T, app *fiber.App, method, target, body string) (int, map[string]any) {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	req.Header.Set("X-Tenant-Id", acmeTenant)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	var decoded map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))

	return resp.StatusCode, decoded
}

func TestWorkflowLifecycleThroughAPI(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	logger := slog.New(slog.DiscardHandler)

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes(notify.SMTP{})

	e := engine.New(reg)

	app := fiber.New()
	api.New(workflow.NewRepository(p), workflow.NewPublishingService(p, e, p.EventDefinitions()), p.ExecutionContextRepository(), p.EventDefinitions(), p.Jobs(), p.RemoteNodeTypes()).Register(app)
	ingest.NewWebhook(p, p.Jobs(), logger).Register(app)

	definition := `{
		"name": "Employee Criado",
		"description": "greets new employees",
		"variables": {"team": "Vendas"},
		"nodes": [
			{"id": "trigger", "name": "trigger", "type": "trigger:webhook", "category": "trigger", "enabled": true, "config": {"webhook_path": "/lifecycle"}},
			{"id": "greet", "name": "greet", "type": "log", "category": "action", "enabled": true, "config": {"message": "hello {{.node_results.trigger.body.employee}} from {{.variables.team}}"}}
		],
		"connections": [{"id": "trigger-greet", "source_port": "%s", "target_port": "greet:main"}]
	}`

	status, broken := request(t, app, http.MethodPost, "/workflows", fmt.Sprintf(definition, "trigger:output"))
	require.Equal(t, http.StatusCreated, status)

	status, problem := request(t, app, http.MethodPost, "/workflows/"+broken["id"].(string)+"/publish", "")
	require.Equal(t, http.StatusUnprocessableEntity, status)
	assert.Contains(t, problem["detail"], `output port "trigger:output" does not exist`)

	status, created := request(t, app, http.MethodPost, "/workflows", fmt.Sprintf(definition, "trigger:success"))
	require.Equal(t, http.StatusCreated, status)
	assert.Equal(t, string(models.WorkflowStatusDraft), created["status"])

	status, published := request(t, app, http.MethodPost, "/workflows/"+created["id"].(string)+"/publish", "")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, string(models.WorkflowStatusPublished), published["status"])

	status, accepted := request(t, app, http.MethodPost, "/webhooks/"+acmeTenant+"/lifecycle", `{"employee": "Ana"}`)
	require.Equal(t, http.StatusAccepted, status)

	executionID := accepted["executions"].([]any)[0].(string)

	workerCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		_ = worker.New(p.Jobs(), p.WorkflowRepository(), e, logger, 10*time.Millisecond).Run(workerCtx)
	}()

	var execution map[string]any

	require.Eventually(t, func() bool {
		_, execution = request(t, app, http.MethodGet, "/executions/"+executionID, "")

		return execution["status"] != string(models.ExecutionStatusRunning)
	}, 10*time.Second, 20*time.Millisecond)

	assert.Equal(t, string(models.ExecutionStatusCompleted), execution["status"], execution["error_message"])
	assert.Equal(t, "hello Ana from Vendas", execution["node_results"].(map[string]any)[greetNodeID].(map[string]any)["data"].(map[string]any)[messageKey])
}
