package postgresql_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/api"
	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/worker"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	employeeCreatedKey   = "employee_created"
	employeeCreatedName  = "Employee created"
	employeeCreatedTopic = "tp_employee_created"
	globexTenant         = "globex"
	companyIDField       = "company_id"
)

func saveEmployeeCreated(ctx context.Context, t *testing.T, p *postgresql.Persistence) {
	t.Helper()

	require.NoError(t, p.EventDefinitions().Save(ctx, &models.EventDefinition{
		Key:        employeeCreatedKey,
		Name:       employeeCreatedName,
		Topic:      employeeCreatedTopic,
		Format:     models.EventFormatJSON,
		TenantFrom: models.TenantFromPayload,
		TenantKey:  companyIDField,
	}))
}

func saveEventWorkflow(ctx context.Context, t *testing.T, p *postgresql.Persistence, tenant, event string, status models.WorkflowStatus) string {
	t.Helper()

	id := uuid.NewString()
	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              id,
		Name:            "onboarding",
		Description:     "greets new employees",
		Status:          status,
		WorkflowGroupID: id,
		TenantID:        tenant,
		Nodes: []*models.WorkflowNode{
			{ID: triggerNodeID, Name: triggerNodeID, Type: models.NodeTypeTriggerEvent, Category: models.CategoryTypeTrigger, Config: map[string]any{"event": event}},
		},
	}))

	return id
}

func TestEventTriggersMatchTheTenantAndTheirTopicsAreConsumed(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	saveEmployeeCreated(ctx, t, p)

	acme := saveEventWorkflow(ctx, t, p, acmeTenant, employeeCreatedKey, models.WorkflowStatusPublished)
	saveEventWorkflow(ctx, t, p, globexTenant, employeeCreatedKey, models.WorkflowStatusPublished)
	saveEventWorkflow(ctx, t, p, acmeTenant, employeeCreatedKey, models.WorkflowStatusDraft)
	saveEventWorkflow(ctx, t, p, acmeTenant, "not_in_catalog", models.WorkflowStatusPublished)
	saveKafkaWorkflow(ctx, t, p, "payments", models.WorkflowStatusPublished)
	saveKafkaWorkflow(ctx, t, p, employeeCreatedTopic, models.WorkflowStatusPublished)

	topics, err := p.KafkaTopics(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"payments", employeeCreatedTopic}, topics)

	matches, err := p.EventTriggers(ctx, employeeCreatedKey, acmeTenant)
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, acme, matches[0].WorkflowID)
	assert.Equal(t, triggerNodeID, matches[0].TriggerNode.ID)

	none, err := p.EventTriggers(ctx, employeeCreatedKey, "initech")
	require.NoError(t, err)
	assert.Empty(t, none)

	definitions, err := p.EventDefinitions().ByTopic(ctx, employeeCreatedTopic)
	require.NoError(t, err)
	require.Len(t, definitions, 1)
	assert.Equal(t, employeeCreatedKey, definitions[0].Key)
}

func TestCatalogEventRunsOnlyTheTenantWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	logger := slog.New(slog.DiscardHandler)

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()

	e := engine.New(reg)

	app := fiber.New()
	api.New(workflow.NewRepository(p), workflow.NewPublishingService(p, e, p.EventDefinitions()), p.ExecutionContextRepository(), p.EventDefinitions(), p.Jobs(), p.RemoteNodeTypes()).Register(app, apiToken)

	status, _ := request(t, app, http.MethodPut, "/platform/event-definitions/"+employeeCreatedKey, `{
		"name": "Employee created",
		"topic": "`+employeeCreatedTopic+`",
		"format": "json",
		"tenant_from": "payload",
		"tenant_key": "company_id"
	}`)
	require.Equal(t, http.StatusOK, status)

	definition := `{
		"name": "Employee created",
		"description": "greets new employees",
		"nodes": [
			{"id": "trigger", "name": "trigger", "type": "trigger:event", "category": "trigger", "config": {"event": "%s"}},
			{"id": "greet", "name": "greet", "type": "log", "category": "action", "config": {"message": "hello {{.node_results.trigger.payload.name}} at {{.node_results.trigger.tenant_id}}"}}
		],
		"connections": [{"id": "trigger-greet", "source_port": "trigger:success", "target_port": "greet:main"}]
	}`

	status, unknown := request(t, app, http.MethodPost, "/workflows", fmt.Sprintf(definition, "employee_fired"))
	require.Equal(t, http.StatusCreated, status)

	status, problem := request(t, app, http.MethodPost, "/workflows/"+unknown["id"].(string)+"/publish", "")
	require.Equal(t, http.StatusUnprocessableEntity, status)
	assert.Contains(t, problem["detail"], `event "employee_fired" is not in the catalog`)

	status, created := request(t, app, http.MethodPost, "/workflows", fmt.Sprintf(definition, employeeCreatedKey))
	require.Equal(t, http.StatusCreated, status)

	workflowID := created["id"].(string)

	status, _ = request(t, app, http.MethodPost, "/workflows/"+workflowID+"/publish", "")
	require.Equal(t, http.StatusOK, status)

	kafka := ingest.NewKafka(p, p.EventDefinitions(), p.Jobs(), ingest.SchemaRegistry{}, logger)
	message := func(offset int64, company string) ingest.KafkaMessage {
		return ingest.KafkaMessage{
			Topic:     employeeCreatedTopic,
			Offset:    offset,
			Value:     []byte(`{"name": "Ana", "company_id": "` + company + `"}`),
			Timestamp: time.Now(),
		}
	}

	require.NoError(t, kafka.Handle(ctx, message(1, globexTenant)))
	require.NoError(t, kafka.Handle(ctx, message(2, acmeTenant)))
	require.NoError(t, kafka.Handle(ctx, message(2, acmeTenant)))

	workerCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		_ = worker.New(p.Jobs(), p.WorkflowRepository(), e, logger, 10*time.Millisecond).Run(workerCtx)
	}()

	var executions []*models.ExecutionContext

	require.Eventually(t, func() bool {
		var err error

		executions, err = p.ExecutionContextRepository().GetExecutionsByWorkflow(ctx, workflowID, 10)

		return err == nil && len(executions) == 1 && executions[0].Status != models.ExecutionStatusRunning
	}, 10*time.Second, 20*time.Millisecond)

	execution := executions[0]
	assert.Equal(t, models.ExecutionStatusCompleted, execution.Status, execution.ErrorMessage)
	assert.Equal(t, "hello Ana at acme", execution.NodeResults[greetNodeID].Data[messageKey])
}
