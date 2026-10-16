package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dukex/operion/pkg/api"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errStore = errors.New("store is down")

type fakeWorkflows struct {
	workflows map[string]*models.Workflow
	created   []*models.Workflow
	err       error
}

func (f *fakeWorkflows) FetchAll(context.Context) ([]*models.Workflow, error) {
	all := make([]*models.Workflow, 0, len(f.workflows))
	for _, wf := range f.workflows {
		all = append(all, wf)
	}

	return all, f.err
}

func (f *fakeWorkflows) FetchByID(_ context.Context, id string) (*models.Workflow, error) {
	if f.err != nil {
		return nil, f.err
	}

	wf, ok := f.workflows[id]
	if !ok {
		return nil, workflow.ErrWorkflowNotFound
	}

	return wf, nil
}

func (f *fakeWorkflows) Create(_ context.Context, wf *models.Workflow) (*models.Workflow, error) {
	if f.err != nil {
		return nil, f.err
	}

	wf.ID = "created"
	f.created = append(f.created, wf)

	return wf, nil
}

type fakePublisher struct {
	err error
}

func (f fakePublisher) PublishWorkflow(_ context.Context, id string) (*models.Workflow, error) {
	if f.err != nil {
		return nil, f.err
	}

	return &models.Workflow{ID: id, Status: models.WorkflowStatusPublished}, nil
}

type fakeExecutions struct {
	executions map[string]*models.ExecutionContext
	err        error
}

func (f fakeExecutions) GetExecutionContext(_ context.Context, id string) (*models.ExecutionContext, error) {
	if f.err != nil {
		return nil, f.err
	}

	execution, ok := f.executions[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", persistence.ErrExecutionNotFound, id)
	}

	return execution, nil
}

type fakeCatalog struct {
	definitions map[string]*models.EventDefinition
	err         error
}

func (f *fakeCatalog) Save(_ context.Context, definition *models.EventDefinition) error {
	if f.err != nil {
		return f.err
	}

	f.definitions[definition.Key] = definition

	return nil
}

func (f *fakeCatalog) Get(_ context.Context, key string) (*models.EventDefinition, error) {
	definition, ok := f.definitions[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", persistence.ErrEventDefinitionNotFound, key)
	}

	return definition, nil
}

func (f *fakeCatalog) List(context.Context) ([]*models.EventDefinition, error) {
	definitions := make([]*models.EventDefinition, 0, len(f.definitions))
	for _, definition := range f.definitions {
		definitions = append(definitions, definition)
	}

	return definitions, f.err
}

type server struct {
	workflows  *fakeWorkflows
	publisher  fakePublisher
	executions fakeExecutions
	catalog    *fakeCatalog
}

func (s server) do(t *testing.T, method, target, body string) (int, map[string]any) {
	t.Helper()

	app := fiber.New()
	api.New(s.workflows, s.publisher, s.executions, s.catalog).Register(app)

	resp, err := app.Test(httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body)))
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	content, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var decoded any
	require.NoError(t, json.Unmarshal(content, &decoded))

	if list, ok := decoded.([]any); ok {
		return resp.StatusCode, map[string]any{"items": list}
	}

	return resp.StatusCode, decoded.(map[string]any)
}

func newServer() server {
	return server{
		workflows: &fakeWorkflows{workflows: map[string]*models.Workflow{"wf": {ID: "wf", Name: "Employee Criado"}}},
		executions: fakeExecutions{executions: map[string]*models.ExecutionContext{
			"exec": {ID: "exec", Status: models.ExecutionStatusCompleted},
		}},
		catalog: &fakeCatalog{definitions: map[string]*models.EventDefinition{}},
	}
}

func TestListAndGetWorkflows(t *testing.T) {
	s := newServer()

	status, body := s.do(t, http.MethodGet, "/workflows", "")
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, body["items"], 1)

	status, body = s.do(t, http.MethodGet, "/workflows/wf", "")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "Employee Criado", body["name"])

	status, body = s.do(t, http.MethodGet, "/workflows/missing", "")
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body["detail"], "workflow not found")
}

func TestCreateWorkflowAlwaysStartsAsDraft(t *testing.T) {
	s := newServer()

	status, body := s.do(t, http.MethodPost, "/workflows", `{"name": "Employee Criado", "description": "greets", "status": "published"}`)

	require.Equal(t, http.StatusCreated, status)
	assert.Equal(t, "created", body["id"])
	assert.Equal(t, string(models.WorkflowStatusDraft), body["status"])
	require.Len(t, s.workflows.created, 1)
	assert.Equal(t, models.WorkflowStatusDraft, s.workflows.created[0].Status)
}

func TestCreateWorkflowRejectsBadInput(t *testing.T) {
	s := newServer()

	status, body := s.do(t, http.MethodPost, "/workflows", `{`)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.NotEmpty(t, body["detail"])

	status, body = s.do(t, http.MethodPost, "/workflows", `{"name": "ab", "description": "too short a name"}`)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body["detail"], "Name")
	assert.Empty(t, s.workflows.created)
}

func TestPublishWorkflow(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{name: "published", status: http.StatusOK},
		{name: "not found", err: fmt.Errorf("%w: wf", workflow.ErrWorkflowNotFound), status: http.StatusNotFound},
		{name: "invalid graph", err: fmt.Errorf("%w: node \"x\" is not reachable from a trigger", workflow.ErrInvalidWorkflow), status: http.StatusUnprocessableEntity},
		{name: "store error", err: errStore, status: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServer()
			s.publisher = fakePublisher{err: tt.err}

			status, body := s.do(t, http.MethodPost, "/workflows/wf/publish", "")

			assert.Equal(t, tt.status, status)

			if tt.err == nil {
				assert.Equal(t, string(models.WorkflowStatusPublished), body["status"])
			} else {
				assert.Equal(t, tt.err.Error(), body["detail"])
			}
		})
	}
}

func TestGetExecution(t *testing.T) {
	s := newServer()

	status, body := s.do(t, http.MethodGet, "/executions/exec", "")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, string(models.ExecutionStatusCompleted), body["status"])

	status, _ = s.do(t, http.MethodGet, "/executions/missing", "")
	assert.Equal(t, http.StatusNotFound, status)
}

func TestStoreErrorsReturnInternalError(t *testing.T) {
	s := newServer()
	s.workflows.err = errStore
	s.executions.err = errStore

	for _, request := range [][2]string{
		{http.MethodGet, "/workflows"},
		{http.MethodGet, "/workflows/wf"},
		{http.MethodGet, "/executions/exec"},
	} {
		status, body := s.do(t, request[0], request[1], "")
		assert.Equal(t, http.StatusInternalServerError, status, request[1])
		assert.Equal(t, errStore.Error(), body["detail"], request[1])
	}

	status, _ := s.do(t, http.MethodPost, "/workflows", `{"name": "Employee Criado", "description": "greets"}`)
	assert.Equal(t, http.StatusInternalServerError, status)
}

const employeeCreated = `{
	"name": "Employee Criado",
	"description": "A company hired someone",
	"topic": "tp_employee_created",
	"format": "json",
	"schema": {"type": "object", "required": ["company_id"]},
	"tenant_from": "payload",
	"tenant_key": "company_id"
}`

func TestSaveAndGetEventDefinition(t *testing.T) {
	s := newServer()

	status, body := s.do(t, http.MethodPut, "/platform/event-definitions/employee_created", employeeCreated)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "employee_created", body["key"])
	assert.Equal(t, "tp_employee_created", s.catalog.definitions["employee_created"].Topic)
	assert.Equal(t, models.TenantFromPayload, s.catalog.definitions["employee_created"].TenantFrom)

	status, body = s.do(t, http.MethodGet, "/platform/event-definitions/employee_created", "")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "company_id", body["tenant_key"])

	status, _ = s.do(t, http.MethodGet, "/platform/event-definitions/missing", "")
	assert.Equal(t, http.StatusNotFound, status)
}

func TestSaveEventDefinitionRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{`},
		{name: "missing topic", body: `{"name": "Employee Criado", "format": "json", "tenant_from": "payload", "tenant_key": "company_id"}`},
		{name: "unknown format", body: `{"name": "Employee Criado", "topic": "t", "format": "xml", "tenant_from": "payload", "tenant_key": "company_id"}`},
		{name: "unknown tenant source", body: `{"name": "Employee Criado", "topic": "t", "format": "json", "tenant_from": "query", "tenant_key": "company_id"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServer()

			status, _ := s.do(t, http.MethodPut, "/platform/event-definitions/employee_created", tt.body)

			assert.Equal(t, http.StatusBadRequest, status)
			assert.Empty(t, s.catalog.definitions)
		})
	}
}

func TestCatalogShowsEventsWithoutRouting(t *testing.T) {
	s := newServer()

	status, _ := s.do(t, http.MethodPut, "/platform/event-definitions/employee_created", employeeCreated)
	require.Equal(t, http.StatusOK, status)

	status, body := s.do(t, http.MethodGet, "/catalog/events", "")
	require.Equal(t, http.StatusOK, status)
	require.Len(t, body["items"], 1)

	event := body["items"].([]any)[0].(map[string]any)
	assert.Equal(t, "Employee Criado", event["name"])
	assert.Equal(t, map[string]any{"type": "object", "required": []any{"company_id"}}, event["schema"])
	assert.NotContains(t, event, "topic")
	assert.NotContains(t, event, "tenant_key")
}

func TestCatalogStoreErrors(t *testing.T) {
	s := newServer()
	s.catalog.err = errStore

	status, _ := s.do(t, http.MethodPut, "/platform/event-definitions/employee_created", employeeCreated)
	assert.Equal(t, http.StatusInternalServerError, status)

	status, _ = s.do(t, http.MethodGet, "/catalog/events", "")
	assert.Equal(t, http.StatusInternalServerError, status)
}
