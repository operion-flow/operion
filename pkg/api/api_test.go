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

const (
	acmeTenant = "acme"
	apiToken   = "operion-test-token"
)

type fakeWorkflows struct {
	workflows map[string]*models.Workflow
	created   []*models.Workflow
	err       error
}

func (f *fakeWorkflows) FetchAll(_ context.Context, tenantID string) ([]*models.Workflow, error) {
	all := make([]*models.Workflow, 0, len(f.workflows))
	for _, wf := range f.workflows {
		if wf.TenantID == tenantID {
			all = append(all, wf)
		}
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
	limits     *[]int
}

func (f fakeExecutions) GetExecutionsByWorkflow(_ context.Context, workflowID string, limit int) ([]*models.ExecutionContext, error) {
	if f.err != nil {
		return nil, f.err
	}

	if f.limits != nil {
		*f.limits = append(*f.limits, limit)
	}

	var found []*models.ExecutionContext

	for _, execution := range f.executions {
		if execution.WorkflowID == workflowID {
			found = append(found, execution)
		}
	}

	return found, nil
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

type resumed struct {
	executionID string
	nodeID      string
	data        map[string]any
}

type fakeResumer struct {
	resumed []resumed
	err     error
}

func (f *fakeResumer) Resume(_ context.Context, executionID, nodeID string, data map[string]any) error {
	if f.err != nil {
		return f.err
	}

	f.resumed = append(f.resumed, resumed{executionID: executionID, nodeID: nodeID, data: data})

	return nil
}

type fakeNodeTypes struct {
	nodeTypes map[string]*models.RemoteNodeType
	err       error
}

func (f *fakeNodeTypes) Save(_ context.Context, nodeType *models.RemoteNodeType) error {
	if f.err != nil {
		return f.err
	}

	f.nodeTypes[nodeType.ID] = nodeType

	return nil
}

func (f *fakeNodeTypes) Get(_ context.Context, id string) (*models.RemoteNodeType, error) {
	nodeType, ok := f.nodeTypes[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", persistence.ErrRemoteNodeTypeNotFound, id)
	}

	return nodeType, nil
}

type server struct {
	tenant     string
	bearer     string
	workflows  *fakeWorkflows
	publisher  fakePublisher
	executions fakeExecutions
	catalog    *fakeCatalog
	resumer    *fakeResumer
	nodeTypes  *fakeNodeTypes
}

func (s server) do(t *testing.T, method, target, body string) (int, map[string]any) {
	t.Helper()

	app := fiber.New()
	api.New(s.workflows, s.publisher, s.executions, s.catalog, s.resumer, s.nodeTypes).Register(app, apiToken)

	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if s.tenant != "" {
		req.Header.Set("X-Tenant-Id", s.tenant)
	}

	if s.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+s.bearer)
	}

	resp, err := app.Test(req)
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
		tenant:    acmeTenant,
		bearer:    apiToken,
		workflows: &fakeWorkflows{workflows: map[string]*models.Workflow{"wf": {ID: "wf", Name: "Employee Criado", TenantID: acmeTenant}}},
		executions: fakeExecutions{executions: map[string]*models.ExecutionContext{
			"exec": {ID: "exec", WorkflowID: "wf", Status: models.ExecutionStatusCompleted},
		}},
		catalog:   &fakeCatalog{definitions: map[string]*models.EventDefinition{}},
		resumer:   &fakeResumer{},
		nodeTypes: &fakeNodeTypes{nodeTypes: map[string]*models.RemoteNodeType{}},
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

const resumeHold = "/executions/exec/nodes/hold/resume"

func TestResumeNode(t *testing.T) {
	s := newServer()

	status, body := s.do(t, http.MethodPost, resumeHold, `{"decision": "approved"}`)
	require.Equal(t, http.StatusAccepted, status, body)
	assert.Equal(t, "exec", body["execution"])

	status, _ = s.do(t, http.MethodPost, resumeHold, "")
	require.Equal(t, http.StatusAccepted, status)

	assert.Equal(t, []resumed{
		{executionID: "exec", nodeID: "hold", data: map[string]any{"decision": "approved"}},
		{executionID: "exec", nodeID: "hold"},
	}, s.resumer.resumed)
}

func TestResumeNodeErrors(t *testing.T) {
	tests := []struct {
		name   string
		target string
		body   string
		err    error
		status int
	}{
		{name: "body is not an object", target: resumeHold, body: `[1]`, status: http.StatusBadRequest},
		{name: "execution not found", target: "/executions/missing/nodes/hold/resume", status: http.StatusNotFound},
		{name: "node not waiting", target: resumeHold, err: fmt.Errorf("%w: hold", persistence.ErrNodeNotWaiting), status: http.StatusConflict},
		{name: "store error", target: resumeHold, err: errStore, status: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServer()
			s.resumer.err = tt.err

			status, body := s.do(t, http.MethodPost, tt.target, tt.body)

			assert.Equal(t, tt.status, status)
			assert.NotEmpty(t, body["detail"])
			assert.Empty(t, s.resumer.resumed)
		})
	}
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

func TestListExecutionsOfAWorkflow(t *testing.T) {
	s := newServer()

	var limits []int

	s.executions.limits = &limits

	status, body := s.do(t, http.MethodGet, "/workflows/wf/executions", "")
	require.Equal(t, http.StatusOK, status)
	require.Len(t, body["items"], 1)
	assert.Equal(t, "exec", body["items"].([]any)[0].(map[string]any)["id"])

	status, _ = s.do(t, http.MethodGet, "/workflows/wf/executions?limit=5", "")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, []int{50, 5}, limits)

	s.workflows.workflows["empty"] = &models.Workflow{ID: "empty", TenantID: acmeTenant}
	status, body = s.do(t, http.MethodGet, "/workflows/empty/executions", "")
	require.Equal(t, http.StatusOK, status)
	assert.Empty(t, body["items"])
}

func TestListExecutionsRejectsBadRequests(t *testing.T) {
	s := newServer()

	status, _ := s.do(t, http.MethodGet, "/workflows/missing/executions", "")
	assert.Equal(t, http.StatusNotFound, status)

	for _, limit := range []string{"0", "501", "ten"} {
		status, body := s.do(t, http.MethodGet, "/workflows/wf/executions?limit="+limit, "")
		assert.Equal(t, http.StatusBadRequest, status, limit)
		assert.Contains(t, body["detail"], "limit must be a number between 1 and 500", limit)
	}

	s.executions.err = errStore
	status, _ = s.do(t, http.MethodGet, "/workflows/wf/executions", "")
	assert.Equal(t, http.StatusInternalServerError, status)
}

func TestTenantRoutesRequireTheTenantHeader(t *testing.T) {
	s := newServer()
	s.tenant = ""

	for _, request := range [][2]string{
		{http.MethodGet, "/workflows"},
		{http.MethodPost, "/workflows"},
		{http.MethodGet, "/workflows/wf"},
		{http.MethodPost, "/workflows/wf/publish"},
		{http.MethodGet, "/workflows/wf/executions"},
		{http.MethodGet, "/executions/exec"},
		{http.MethodPost, resumeHold},
	} {
		status, body := s.do(t, request[0], request[1], `{"name": "Employee Criado", "description": "greets"}`)
		assert.Equal(t, http.StatusBadRequest, status, request[1])
		assert.Equal(t, "X-Tenant-Id header is required", body["detail"], request[1])
	}
}

func TestOtherTenantsSeeNothing(t *testing.T) {
	s := newServer()
	s.tenant = "globex"

	status, body := s.do(t, http.MethodGet, "/workflows", "")
	require.Equal(t, http.StatusOK, status)
	assert.Empty(t, body["items"])

	for _, request := range [][2]string{
		{http.MethodGet, "/workflows/wf"},
		{http.MethodPost, "/workflows/wf/publish"},
		{http.MethodGet, "/workflows/wf/executions"},
		{http.MethodGet, "/executions/exec"},
		{http.MethodPost, resumeHold},
	} {
		status, _ := s.do(t, request[0], request[1], "")
		assert.Equal(t, http.StatusNotFound, status, request[1])
	}

	assert.Empty(t, s.resumer.resumed)

	status, body = s.do(t, http.MethodGet, "/executions/exec", "")
	require.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body["detail"], "execution context not found")
}

func TestCreateWorkflowBelongsToTheCallerTenant(t *testing.T) {
	s := newServer()
	s.tenant = "globex"

	status, _ := s.do(t, http.MethodPost, "/workflows", `{"name": "Employee Criado", "description": "greets", "tenant_id": "acme"}`)

	require.Equal(t, http.StatusCreated, status)
	require.Len(t, s.workflows.created, 1)
	assert.Equal(t, "globex", s.workflows.created[0].TenantID)
}

func TestCreateWorkflowVersionNeedsAGroupOfTheCallerTenant(t *testing.T) {
	const (
		acmeGroup    = "6f1c2d4e-8a7b-4c3d-9e2f-1a2b3c4d5e6f"
		globexGroup  = "0a9b8c7d-6e5f-4a3b-8c2d-1e0f9a8b7c6d"
		unknownGroup = "11111111-2222-4333-8444-555555555555"
	)

	tests := []struct {
		name   string
		group  string
		status int
	}{
		{name: "own group", group: acmeGroup, status: http.StatusCreated},
		{name: "group of another tenant", group: globexGroup, status: http.StatusNotFound},
		{name: "unknown group", group: unknownGroup, status: http.StatusNotFound},
		{name: "group is not a uuid", group: "wf", status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServer()
			s.workflows.workflows[acmeGroup] = &models.Workflow{ID: acmeGroup, TenantID: acmeTenant}
			s.workflows.workflows[globexGroup] = &models.Workflow{ID: globexGroup, TenantID: "globex"}

			status, _ := s.do(t, http.MethodPost, "/workflows", `{"name": "Employee Criado", "description": "greets", "workflow_group_id": "`+tt.group+`"}`)

			require.Equal(t, tt.status, status)

			if tt.status != http.StatusCreated {
				assert.Empty(t, s.workflows.created)

				return
			}

			require.Len(t, s.workflows.created, 1)
			assert.Equal(t, acmeGroup, s.workflows.created[0].WorkflowGroupID)
		})
	}
}

func TestSaveAndGetNodeType(t *testing.T) {
	s := newServer()

	status, body := s.do(t, http.MethodPut, "/platform/node-types/create_benefit", `{"name": "Criar benefício", "url": "https://benefits.internal/operion"}`)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "create_benefit", body["id"])
	assert.Equal(t, []any{"success", "error"}, body["outputs"])
	assert.InDelta(t, 30, body["timeout_seconds"], 0)

	status, body = s.do(t, http.MethodGet, "/platform/node-types/create_benefit", "")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "https://benefits.internal/operion", body["url"])

	status, _ = s.do(t, http.MethodGet, "/platform/node-types/missing", "")
	assert.Equal(t, http.StatusNotFound, status)
}

func TestNodeTypeSecretIsWriteOnly(t *testing.T) {
	s := newServer()

	status, body := s.do(t, http.MethodPut, "/platform/node-types/create_benefit", `{"name": "Criar benefício", "url": "https://benefits", "secret": "a-secret-of-sixteen-bytes"}`)
	require.Equal(t, http.StatusOK, status, body)
	assert.NotContains(t, body, "secret")
	assert.Equal(t, "a-secret-of-sixteen-bytes", s.nodeTypes.nodeTypes["create_benefit"].Secret)

	status, body = s.do(t, http.MethodGet, "/platform/node-types/create_benefit", "")
	require.Equal(t, http.StatusOK, status)
	assert.NotContains(t, body, "secret")
	assert.Equal(t, "a-secret-of-sixteen-bytes", s.nodeTypes.nodeTypes["create_benefit"].Secret)
}

func TestSaveNodeTypeRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{`},
		{name: "missing url", body: `{"name": "Criar benefício"}`},
		{name: "not an http url", body: `{"name": "Criar benefício", "url": "ftp://benefits"}`},
		{name: "timeout too long", body: `{"name": "Criar benefício", "url": "https://benefits", "timeout_seconds": 301}`},
		{name: "short secret", body: `{"name": "Criar benefício", "url": "https://benefits", "secret": "short"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServer()

			status, _ := s.do(t, http.MethodPut, "/platform/node-types/create_benefit", tt.body)

			assert.Equal(t, http.StatusBadRequest, status)
			assert.Empty(t, s.nodeTypes.nodeTypes)
		})
	}
}

func TestSaveNodeTypeStoreError(t *testing.T) {
	s := newServer()
	s.nodeTypes.err = errStore

	status, _ := s.do(t, http.MethodPut, "/platform/node-types/create_benefit", `{"name": "Criar benefício", "url": "https://benefits"}`)

	assert.Equal(t, http.StatusInternalServerError, status)
}

var allRoutes = [][2]string{
	{http.MethodGet, "/workflows"},
	{http.MethodPost, "/workflows"},
	{http.MethodGet, "/workflows/wf"},
	{http.MethodPost, "/workflows/wf/publish"},
	{http.MethodGet, "/workflows/wf/executions"},
	{http.MethodGet, "/executions/exec"},
	{http.MethodPost, resumeHold},
	{http.MethodPut, "/platform/event-definitions/employee_created"},
	{http.MethodGet, "/platform/event-definitions/employee_created"},
	{http.MethodGet, "/catalog/events"},
	{http.MethodPut, "/platform/node-types/create_benefit"},
	{http.MethodGet, "/platform/node-types/create_benefit"},
}

func TestEveryRouteRequiresTheBearerToken(t *testing.T) {
	for _, bearer := range []string{"", "wrong", apiToken + "x"} {
		s := newServer()
		s.bearer = bearer

		for _, request := range allRoutes {
			status, body := s.do(t, request[0], request[1], `{"name": "Employee Criado", "description": "greets"}`)
			assert.Equal(t, http.StatusUnauthorized, status, request[1])
			assert.Equal(t, "a valid bearer token is required", body["detail"], request[1])
		}

		assert.Empty(t, s.workflows.created)
		assert.Empty(t, s.resumer.resumed)
		assert.Empty(t, s.nodeTypes.nodeTypes)
	}
}

func TestUnauthorizedAnswersAskForABearerToken(t *testing.T) {
	app := fiber.New()
	s := newServer()
	api.New(s.workflows, s.publisher, s.executions, s.catalog, s.resumer, s.nodeTypes).Register(app, apiToken)

	resp, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/workflows", nil))
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "Bearer", resp.Header.Get("WWW-Authenticate"))
}

func TestAnEmptyConfiguredTokenLetsNobodyIn(t *testing.T) {
	app := fiber.New()
	s := newServer()
	api.New(s.workflows, s.publisher, s.executions, s.catalog, s.resumer, s.nodeTypes).Register(app, "")

	for _, header := range []string{"", "Bearer ", "Bearer"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/workflows", nil)
		req.Header.Set("X-Tenant-Id", acmeTenant)
		req.Header.Set("Authorization", header)

		resp, err := app.Test(req)
		require.NoError(t, err)

		_ = resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, header)
	}
}
