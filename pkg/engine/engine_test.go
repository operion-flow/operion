package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	"github.com/dukex/operion/pkg/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	errorPort   = "error"
	failingType = "failing"
)

type stubNode struct {
	protocol.Node

	outputs map[string]models.NodeResult
	err     error
}

func (s stubNode) Execute(models.ExecutionContext, map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	return s.outputs, s.err
}

type waiterNode struct {
	stubNode

	at  time.Time
	err error
}

func (w waiterNode) WaitUntil(models.ExecutionContext, map[string]models.NodeResult) (time.Time, error) {
	return w.at, w.err
}

type creator struct {
	registry *registry.Registry
	stubs    map[string]protocol.Node
}

func (c creator) CreateNode(ctx context.Context, nodeType, nodeID string, config map[string]any) (protocol.Node, error) {
	if stub, ok := c.stubs[nodeType]; ok {
		return stub, nil
	}

	return c.registry.CreateNode(ctx, nodeType, nodeID, config)
}

func newEngine(stubs map[string]protocol.Node) *engine.Engine {
	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()

	return engine.New(creator{registry: reg, stubs: stubs})
}

func node(id, nodeType string, config map[string]any) *models.WorkflowNode {
	return &models.WorkflowNode{ID: id, Name: id, Type: nodeType, Config: config}
}

func trigger() *models.WorkflowNode {
	n := node("trigger", "trigger:webhook", map[string]any{"webhook_path": "/hook"})
	n.Category = models.CategoryTypeTrigger

	return n
}

func logNode(id string) *models.WorkflowNode {
	return node(id, "log", map[string]any{"message": id})
}

func connect(source, target string) *models.Connection {
	return &models.Connection{ID: source + "->" + target, SourcePort: source, TargetPort: target}
}

func workflow(nodes []*models.WorkflowNode, connections ...*models.Connection) *models.Workflow {
	return &models.Workflow{ID: "wf", Name: "workflow", Nodes: nodes, Connections: connections}
}

func run(t *testing.T, e *engine.Engine, wf *models.Workflow, body map[string]any) *models.ExecutionContext {
	t.Helper()

	data := map[string]any{"body": body}
	execution := &models.ExecutionContext{ID: "exec", WorkflowID: wf.ID, Status: models.ExecutionStatusRunning, TriggerData: data}

	deferred, err := e.Run(t.Context(), wf, execution, engine.TriggerActivation("trigger", data))
	require.NoError(t, err)
	require.Empty(t, deferred)

	return execution
}

func TestRunCompletesLinearFlow(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{
			trigger(),
			node("greet", "transform", map[string]any{"expression": "hello {{.node_results.trigger.body.name}}"}),
			logNode("done"),
		},
		connect("trigger:success", "greet:main"),
		connect("greet:success", "done:main"),
	)

	execution := run(t, newEngine(nil), wf, map[string]any{"name": "Ana"})

	assert.Equal(t, models.ExecutionStatusCompleted, execution.Status)
	assert.Empty(t, execution.ErrorMessage)
	assert.NotNil(t, execution.CompletedAt)
	assert.Equal(t, "hello Ana", execution.NodeResults["greet"].Data["result"])
	assert.Equal(t, "greet", execution.NodeResults["greet"].NodeID)
	assert.Contains(t, execution.NodeResults, "done")
}

func TestRunCompletesWhenTakenBranchHasNoConnection(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{
			trigger(),
			node("check", "conditional", map[string]any{"condition": "{{.node_results.trigger.body.active}}"}),
			logNode("active"),
		},
		connect("trigger:success", "check:main"),
		connect("check:true", "active:main"),
	)

	execution := run(t, newEngine(nil), wf, map[string]any{"active": false})

	assert.Equal(t, models.ExecutionStatusCompleted, execution.Status)
	assert.Equal(t, false, execution.NodeResults["check"].Data["condition_result"])
	assert.NotContains(t, execution.NodeResults, "active")
}

func TestRunExecutesEveryParallelBranch(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), logNode("left"), logNode("right")},
		connect("trigger:success", "left:main"),
		connect("trigger:success", "right:main"),
	)

	execution := run(t, newEngine(nil), wf, nil)

	assert.Equal(t, models.ExecutionStatusCompleted, execution.Status)
	assert.Contains(t, execution.NodeResults, "left")
	assert.Contains(t, execution.NodeResults, "right")
}

func TestRunFollowsConnectedErrorPort(t *testing.T) {
	stubs := erroring()
	wf := workflow(
		[]*models.WorkflowNode{trigger(), node("step", failingType, nil), logNode("recover")},
		connect("trigger:success", "step:main"),
		connect("step:error", "recover:main"),
	)

	execution := run(t, newEngine(stubs), wf, nil)

	assert.Equal(t, models.ExecutionStatusCompleted, execution.Status)
	assert.Contains(t, execution.NodeResults, "step")
	assert.Contains(t, execution.NodeResults, "recover")
}

func TestRunFailsOnUnhandledErrorPort(t *testing.T) {
	tests := []struct {
		name    string
		result  models.NodeResult
		message string
	}{
		{name: "error field", result: models.NodeResult{Error: "kaboom"}, message: `node "step": kaboom`},
		{name: "error in data", result: models.NodeResult{Data: map[string]any{"error": boom}}, message: `node "step": boom`},
		{name: "no message", result: models.NodeResult{}, message: `node "step": failed without an error message`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubs := map[string]protocol.Node{failingType: stubNode{outputs: map[string]models.NodeResult{errorPort: tt.result}}}
			wf := workflow(
				[]*models.WorkflowNode{trigger(), node("step", failingType, nil), logNode("after")},
				connect("trigger:success", "step:main"),
				connect("step:success", "after:main"),
			)

			execution := run(t, newEngine(stubs), wf, nil)

			assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
			assert.Equal(t, tt.message, execution.ErrorMessage)
			assert.NotNil(t, execution.CompletedAt)
		})
	}
}

func TestRunFailsWhenNodeReturnsError(t *testing.T) {
	stubs := map[string]protocol.Node{failingType: stubNode{err: errors.New("exploded")}}
	wf := workflow(
		[]*models.WorkflowNode{trigger(), node("step", failingType, nil)},
		connect("trigger:success", "step:main"),
	)

	execution := run(t, newEngine(stubs), wf, nil)

	assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
	assert.Equal(t, `node "step": exploded`, execution.ErrorMessage)
}

func TestRunFailsOnUnknownNodeType(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), node("step", "missing", nil)},
		connect("trigger:success", "step:main"),
	)

	execution := run(t, newEngine(nil), wf, nil)

	assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
	assert.Contains(t, execution.ErrorMessage, `node "step"`)
	assert.Contains(t, execution.ErrorMessage, registry.ErrNodeNotRegistered.Error())
}

func TestRunFailsOnConnectionToMissingNode(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		message string
	}{
		{name: "unknown node", target: "ghost:main", message: `node "ghost" not found in workflow`},
		{name: "malformed port", target: "ghost", message: `node "" not found in workflow`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wf := workflow([]*models.WorkflowNode{trigger()}, connect("trigger:success", tt.target))

			execution := run(t, newEngine(nil), wf, nil)

			assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
			assert.Equal(t, tt.message, execution.ErrorMessage)
		})
	}
}

func TestRunStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	wf := workflow([]*models.WorkflowNode{trigger()})
	execution := &models.ExecutionContext{ID: "exec", Status: models.ExecutionStatusRunning}

	_, err := newEngine(nil).Run(ctx, wf, execution, engine.TriggerActivation("trigger", nil))

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, models.ExecutionStatusRunning, execution.Status)
	assert.Nil(t, execution.CompletedAt)
}

const holdType = "hold"

func holding(at time.Time, err error) map[string]protocol.Node {
	return map[string]protocol.Node{holdType: waiterNode{
		stubNode: stubNode{outputs: map[string]models.NodeResult{"success": {Data: map[string]any{"held": true}}}},
		at:       at,
		err:      err,
	}}
}

func TestRunDefersWaitingNodes(t *testing.T) {
	at := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	wf := workflow(
		[]*models.WorkflowNode{trigger(), node("hold", holdType, nil), logNode("after"), logNode("now")},
		connect("trigger:success", "hold:main"),
		connect("trigger:success", "now:main"),
		connect("hold:success", "after:main"),
	)
	data := map[string]any{"body": map[string]any{"name": "Ana"}}
	execution := &models.ExecutionContext{ID: "exec", WorkflowID: wf.ID, Status: models.ExecutionStatusRunning, TriggerData: data}

	deferred, err := newEngine(holding(at, nil)).Run(t.Context(), wf, execution, engine.TriggerActivation("trigger", data))

	require.NoError(t, err)
	require.Len(t, deferred, 1)
	assert.Equal(t, "hold", deferred[0].NodeID)
	assert.Equal(t, engine.ResumePort, deferred[0].Port)
	assert.Equal(t, execution.NodeResults["trigger"].Data, deferred[0].Data)
	assert.Equal(t, at, deferred[0].RunAt)
	assert.Equal(t, models.ExecutionStatusRunning, execution.Status)
	assert.Nil(t, execution.CompletedAt)
	assert.Equal(t, string(models.NodeStatusWaiting), execution.NodeResults["hold"].Status)
	assert.Equal(t, "2026-10-20T09:00:00Z", execution.NodeResults["hold"].Data["resume_at"])
	assert.Contains(t, execution.NodeResults, "now")
	assert.NotContains(t, execution.NodeResults, "after")
}

func TestRunResumesAWaitingNode(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), node("hold", holdType, nil), logNode("after")},
		connect("trigger:success", "hold:main"),
		connect("hold:success", "after:main"),
	)
	execution := &models.ExecutionContext{ID: "exec", WorkflowID: wf.ID, Status: models.ExecutionStatusRunning}

	deferred, err := newEngine(holding(time.Now(), nil)).Run(t.Context(), wf, execution,
		engine.Activation{NodeID: "hold", Port: engine.ResumePort, Data: map[string]any{"approved": true}})

	require.NoError(t, err)
	assert.Empty(t, deferred)
	assert.Equal(t, models.ExecutionStatusCompleted, execution.Status)
	assert.Equal(t, true, execution.NodeResults["hold"].Data["held"])
	assert.Contains(t, execution.NodeResults, "after")
}

func TestRunFailsWhenTheWaitCannotBeComputed(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{trigger(), node("hold", holdType, nil)},
		connect("trigger:success", "hold:main"),
	)

	execution := run(t, newEngine(holding(time.Time{}, errors.New("no start date"))), wf, nil)

	assert.Equal(t, models.ExecutionStatusFailed, execution.Status)
	assert.Equal(t, `node "hold": no start date`, execution.ErrorMessage)
}

func TestRunHoldsANodeThatSuspends(t *testing.T) {
	until := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	stubs := map[string]protocol.Node{holdType: stubNode{err: &protocol.SuspendError{Until: until, Data: map[string]any{"answer_by": "tomorrow"}}}}
	wf := workflow(
		[]*models.WorkflowNode{trigger(), node("hold", holdType, nil), logNode("after")},
		connect("trigger:success", "hold:main"),
		connect("hold:success", "after:main"),
	)
	execution := &models.ExecutionContext{ID: "exec", WorkflowID: wf.ID, Status: models.ExecutionStatusRunning}

	deferred, err := newEngine(stubs).Run(t.Context(), wf, execution, engine.TriggerActivation("trigger", nil))

	require.NoError(t, err)
	require.Equal(t, []engine.Deferred{{Activation: engine.Activation{NodeID: "hold", Port: engine.ResumePort}, RunAt: until}}, deferred)
	assert.Equal(t, models.ExecutionStatusRunning, execution.Status)
	assert.Equal(t, string(models.NodeStatusWaiting), execution.NodeResults["hold"].Status)
	assert.Equal(t, map[string]any{"answer_by": "tomorrow"}, execution.NodeResults["hold"].Data)
	assert.NotContains(t, execution.NodeResults, "after")
}
