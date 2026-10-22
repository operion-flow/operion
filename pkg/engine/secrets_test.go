package engine_test

import (
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

func withSecrets(stubs map[string]protocol.Node) *engine.Engine {
	e := newEngine(stubs)
	e.UseSecrets(map[string]string{"crm_token": crmToken, "pin": "1234"})

	return e
}

func TestRunRendersSecretsAndMasksThemInResults(t *testing.T) {
	wf := workflow(
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

func TestRunFailsOnAMissingSecret(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), node("header", "transform", map[string]any{expressionKey: `{{secret "database_url"}}`})},
		connect("trigger:success", "header:main"),
	)

	execution := run(t, withSecrets(nil), wf, nil)

	assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
	assert.Contains(t, execution.ErrorMessage, "secret not found: database_url")
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

			wf := workflow([]*models.WorkflowNode{trigger(), step}, connect("trigger:success", "step:main"))
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
