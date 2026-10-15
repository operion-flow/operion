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

type server struct {
	workflows  *fakeWorkflows
	publisher  fakePublisher
	executions fakeExecutions
}

func (s server) do(t *testing.T, method, target, body string) (int, map[string]any) {
	t.Helper()

	app := fiber.New()
	api.New(s.workflows, s.publisher, s.executions).Register(app)

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
