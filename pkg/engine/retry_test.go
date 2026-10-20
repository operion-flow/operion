package engine_test

import (
	"errors"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	stepID      = "step"
	boom        = "boom"
	executionID = "exec"
)

func retrying(n *models.WorkflowNode, attempts int, initial, limit string) *models.WorkflowNode {
	n.Retry = &models.RetryPolicy{Attempts: attempts, Initial: initial, Max: limit}

	return n
}

func erroring() map[string]protocol.Node {
	return map[string]protocol.Node{failingType: stubNode{outputs: map[string]models.NodeResult{errorPort: {Data: map[string]any{"error": boom}}}}}
}

func exploding() map[string]protocol.Node {
	return map[string]protocol.Node{failingType: stubNode{err: errors.New("exploded")}}
}

func running(wf *models.Workflow) *models.ExecutionContext {
	return &models.ExecutionContext{ID: executionID, WorkflowID: wf.ID, Status: models.ExecutionStatusRunning}
}

func runFrom(t *testing.T, e *engine.Engine, wf *models.Workflow, execution *models.ExecutionContext, start engine.Activation) []engine.Deferred {
	t.Helper()

	deferred, err := e.Run(t.Context(), wf, execution, start)
	require.NoError(t, err)

	return deferred
}

func TestRunRetriesAFailedNodeWithBackoffUntilTheAttemptsRunOut(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), retrying(node(stepID, failingType, nil), 3, "1s", "1m"), logNode("after")},
		connect("trigger:success", "step:main"),
		connect("step:success", "after:main"),
	)
	execution := running(wf)
	e := newEngine(exploding())

	deferred := runFrom(t, e, wf, execution, engine.TriggerActivation("trigger", map[string]any{}))

	require.Len(t, deferred, 1)
	assert.Equal(t, engine.Activation{NodeID: stepID, Port: "main", Data: execution.NodeResults["trigger"].Data, Attempt: 1}, deferred[0].Activation)
	assert.WithinDuration(t, time.Now().Add(time.Second), deferred[0].RunAt, 500*time.Millisecond)
	assert.Equal(t, models.ExecutionStatusRunning, execution.Status)
	assert.Equal(t, string(models.NodeStatusRetrying), execution.NodeResults[stepID].Status)
	assert.Equal(t, "exploded", execution.NodeResults[stepID].Error)
	assert.Equal(t, 1, execution.NodeResults[stepID].Data["attempt"])

	deferred = runFrom(t, e, wf, execution, deferred[0].Activation)

	require.Len(t, deferred, 1)
	assert.Equal(t, 2, deferred[0].Attempt)
	assert.WithinDuration(t, time.Now().Add(2*time.Second), deferred[0].RunAt, 500*time.Millisecond)

	deferred = runFrom(t, e, wf, execution, deferred[0].Activation)

	assert.Empty(t, deferred)
	assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
	assert.Equal(t, `node "step": exploded`, execution.ErrorMessage)
	assert.NotContains(t, execution.NodeResults, "after")
}

func TestRunCapsTheBackoffAtMax(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), retrying(node(stepID, failingType, nil), 100, "1s", "1m")},
		connect("trigger:success", "step:main"),
	)
	execution := running(wf)

	deferred := runFrom(t, newEngine(exploding()), wf, execution, engine.Activation{NodeID: stepID, Port: "main", Attempt: 80})

	require.Len(t, deferred, 1)
	assert.Equal(t, 81, deferred[0].Attempt)
	assert.WithinDuration(t, time.Now().Add(time.Minute), deferred[0].RunAt, 500*time.Millisecond)
}

func TestRunRetriesAnErrorPortBeforeFollowingIt(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), retrying(node(stepID, failingType, nil), 2, "1s", "1s"), logNode("recover")},
		connect("trigger:success", "step:main"),
		connect("step:error", "recover:main"),
	)
	execution := running(wf)
	e := newEngine(erroring())

	deferred := runFrom(t, e, wf, execution, engine.TriggerActivation("trigger", map[string]any{}))

	require.Len(t, deferred, 1)
	assert.Equal(t, boom, execution.NodeResults[stepID].Error)
	assert.NotContains(t, execution.NodeResults, "recover")

	deferred = runFrom(t, e, wf, execution, deferred[0].Activation)

	assert.Empty(t, deferred)
	assert.Equal(t, models.ExecutionStatusCompleted, execution.Status)
	assert.Contains(t, execution.NodeResults, "recover")
}

func TestRunDoesNotRetryNodesThatSucceedOrHaveABrokenPolicy(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), retrying(logNode("greet"), 3, "1s", "1m"), retrying(node(stepID, failingType, nil), 3, "soon", "1m")},
		connect("trigger:success", "greet:main"),
		connect("greet:success", "step:main"),
	)
	execution := running(wf)

	deferred := runFrom(t, newEngine(exploding()), wf, execution, engine.TriggerActivation("trigger", map[string]any{}))

	assert.Empty(t, deferred)
	assert.Equal(t, string(models.NodeStatusSuccess), execution.NodeResults["greet"].Status)
	assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
	assert.Equal(t, `node "step": exploded`, execution.ErrorMessage)
}
