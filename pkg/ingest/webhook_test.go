package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/models"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTriggers struct {
	matches map[string][]models.TriggerNodeMatch
	err     error
}

func (f fakeTriggers) WebhookTriggers(_ context.Context, tenantID, path string) ([]models.TriggerNodeMatch, error) {
	return f.matches[tenantID+path], f.err
}

type started struct {
	execution *models.ExecutionContext
	job       models.Job
}

type fakeStarter struct {
	started []started
	err     error
}

func (f *fakeStarter) Start(_ context.Context, execution *models.ExecutionContext, job models.Job) error {
	if f.err != nil {
		return f.err
	}

	f.started = append(f.started, started{execution: execution, job: job})

	return nil
}

func match(workflowID, nodeID string) models.TriggerNodeMatch {
	return models.TriggerNodeMatch{WorkflowID: workflowID, TriggerNode: &models.WorkflowNode{ID: nodeID}}
}

func post(t *testing.T, triggers fakeTriggers, starter *fakeStarter, target, body string) (int, string) {
	t.Helper()

	app := fiber.New()
	ingest.NewWebhook(triggers, starter, slog.New(slog.DiscardHandler)).Register(app)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	content, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(content)
}

func TestWebhookStartsOneExecutionPerMatchingWorkflow(t *testing.T) {
	triggers := fakeTriggers{matches: map[string][]models.TriggerNodeMatch{
		"acme/employees/created": {match("wf-1", "trigger"), match("wf-2", "hook")},
	}}
	starter := &fakeStarter{}

	status, body := post(t, triggers, starter, "/webhooks/acme/employees/created?source=hr", `{"name": "Ana"}`)

	require.Equal(t, http.StatusAccepted, status)
	require.Len(t, starter.started, 2)

	var response struct {
		Executions []string `json:"executions"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &response))
	assert.Equal(t, []string{starter.started[0].execution.ID, starter.started[1].execution.ID}, response.Executions)

	first := starter.started[0]
	assert.Equal(t, "wf-1", first.execution.WorkflowID)
	assert.Equal(t, models.ExecutionStatusRunning, first.execution.Status)
	assert.Equal(t, first.execution.ID, first.job.ExecutionID)
	assert.Equal(t, "trigger", first.job.NodeID)
	assert.Equal(t, "external", first.job.Port)
	assert.Equal(t, map[string]any{"name": "Ana"}, first.job.Data["body"])
	assert.Equal(t, http.MethodPost, first.job.Data["method"])
	assert.Equal(t, "/webhooks/acme/employees/created?source=hr", first.job.Data["url"])
	assert.Equal(t, map[string]any{"source": "hr"}, first.job.Data["query"])
	assert.Equal(t, "application/json", first.job.Data["headers"].(map[string]any)["Content-Type"])
	assert.Equal(t, first.job.Data, first.execution.TriggerData)
	assert.Equal(t, "hook", starter.started[1].job.NodeID)
}

func TestWebhookAcceptsEmptyBody(t *testing.T) {
	triggers := fakeTriggers{matches: map[string][]models.TriggerNodeMatch{"acme/ping": {match("wf", "trigger")}}}
	starter := &fakeStarter{}

	status, _ := post(t, triggers, starter, "/webhooks/acme/ping", "")

	require.Equal(t, http.StatusAccepted, status)
	require.Len(t, starter.started, 1)
	assert.Nil(t, starter.started[0].job.Data["body"])
}

func TestWebhookRejectsRequests(t *testing.T) {
	errLookup := errors.New("lookup failed")
	errStart := errors.New("start failed")
	matching := map[string][]models.TriggerNodeMatch{"acme/hook": {match("wf", "trigger")}}

	tests := []struct {
		name     string
		triggers fakeTriggers
		starter  *fakeStarter
		body     string
		status   int
	}{
		{name: "no workflow listens", triggers: fakeTriggers{}, starter: &fakeStarter{}, body: `{}`, status: http.StatusNotFound},
		{name: "invalid json", triggers: fakeTriggers{matches: matching}, starter: &fakeStarter{}, body: `{`, status: http.StatusBadRequest},
		{name: "lookup error", triggers: fakeTriggers{err: errLookup}, starter: &fakeStarter{}, body: `{}`, status: http.StatusInternalServerError},
		{name: "start error", triggers: fakeTriggers{matches: matching}, starter: &fakeStarter{err: errStart}, body: `{}`, status: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _ := post(t, tt.triggers, tt.starter, "/webhooks/acme/hook", tt.body)

			assert.Equal(t, tt.status, status)
			assert.Empty(t, tt.starter.started)
		})
	}
}

func TestWebhookOnlyStartsWorkflowsOfThePathTenant(t *testing.T) {
	triggers := fakeTriggers{matches: map[string][]models.TriggerNodeMatch{"acme/employees": {match("wf", "trigger")}}}
	starter := &fakeStarter{}

	status, _ := post(t, triggers, starter, "/webhooks/globex/employees", `{}`)

	assert.Equal(t, http.StatusNotFound, status)
	assert.Empty(t, starter.started)
}
