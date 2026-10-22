package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	crmToken      = "s3cr3t-crm-token"
	expressionKey = "expression"
)

var errVault = errors.New("vault is sealed")

type tenantSecrets map[string]map[string]string

func (s tenantSecrets) Values(_ context.Context, tenantID string) (map[string]string, error) {
	return s[tenantID], nil
}

type failingSecrets struct{}

func (failingSecrets) Values(context.Context, string) (map[string]string, error) {
	return nil, errVault
}

func withSecrets(stubs map[string]protocol.Node) *engine.Engine {
	e := newEngine(stubs)
	e.UseSecrets(tenantSecrets{"acme": {"crm_token": crmToken, "pin": "1234"}})

	return e
}

func tenantWorkflow(tenant string, nodes []*models.WorkflowNode, connections ...*models.Connection) *models.Workflow {
	wf := workflow(nodes, connections...)
	wf.TenantID = tenant

	return wf
}

func TestRunRendersTheTenantSecretsAndMasksThemInResults(t *testing.T) {
	wf := tenantWorkflow("acme",
		[]*models.WorkflowNode{
			trigger(),
			node("header", "transform", map[string]any{expressionKey: `Bearer {{secret "crm_token"}}`}),
			node("short", "transform", map[string]any{expressionKey: `{{secret "pin"}}`}),
		},
		connect("trigger:success", "header:main"),
		connect("header:success", "short:main"),
	)

	execution := run(t, withSecrets(nil), wf, nil)

	require.Equal(t, models.ExecutionStatusCompleted, execution.Status, execution.ErrorMessage)
	assert.Equal(t, "Bearer ***", execution.NodeResults["header"].Data["result"])
	assert.InDelta(t, 1234, execution.NodeResults["short"].Data["result"], 0)
}

func TestRunOnlyReadsTheSecretsOfTheWorkflowTenant(t *testing.T) {
	wf := tenantWorkflow("globex",
		[]*models.WorkflowNode{trigger(), node("header", "transform", map[string]any{expressionKey: `{{secret "crm_token"}}`})},
		connect("trigger:success", "header:main"),
	)

	execution := run(t, withSecrets(nil), wf, nil)

	assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
	assert.Contains(t, execution.ErrorMessage, "secret not found: crm_token")
}

func TestRunMasksSecretsInErrors(t *testing.T) {
	tests := []struct {
		name  string
		stub  stubNode
		retry *models.RetryPolicy
	}{
		{name: "returned error", stub: stubNode{err: errors.New("call with " + crmToken + " failed")}},
		{name: "error port", stub: stubNode{outputs: map[string]models.NodeResult{errorPort: {Data: map[string]any{errorPort: "call with " + crmToken + " failed"}}}}},
		{name: "retry", stub: stubNode{err: errors.New("call with " + crmToken + " failed")}, retry: &models.RetryPolicy{Attempts: 2, Initial: "1s", Max: "1s"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := node("step", failingType, nil)
			step.Retry = tt.retry

			wf := tenantWorkflow("acme", []*models.WorkflowNode{trigger(), step}, connect("trigger:success", "step:main"))
			execution := &models.ExecutionContext{ID: executionID, WorkflowID: wf.ID, Status: models.ExecutionStatusRunning}

			_, err := withSecrets(map[string]protocol.Node{failingType: tt.stub}).Run(t.Context(), wf, execution, engine.TriggerActivation("trigger", nil))
			require.NoError(t, err)

			stored := execution.ErrorMessage + execution.NodeResults["step"].Error
			if message, ok := execution.NodeResults["step"].Data[errorPort].(string); ok {
				stored += message
			}

			assert.NotContains(t, stored, crmToken)
			assert.Contains(t, stored, "call with *** failed")
		})
	}
}

func TestRunFailsWhenTheSecretsCannotBeLoaded(t *testing.T) {
	e := newEngine(nil)
	e.UseSecrets(failingSecrets{})

	wf := tenantWorkflow("acme", []*models.WorkflowNode{trigger(), logNode("done")}, connect("trigger:success", "done:main"))
	execution := &models.ExecutionContext{ID: executionID, WorkflowID: wf.ID, Status: models.ExecutionStatusRunning}

	_, err := e.Run(t.Context(), wf, execution, engine.TriggerActivation("trigger", nil))

	require.ErrorIs(t, err, errVault)
	assert.Empty(t, execution.NodeResults)
}
