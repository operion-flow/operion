package worker_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pending struct {
	job       models.Job
	execution *models.ExecutionContext
}

type fakeJobs struct {
	pending []pending
	done    []*models.ExecutionContext
	errs    []error
	onEmpty func()
}

func (f *fakeJobs) ProcessNext(
	ctx context.Context,
	process func(ctx context.Context, job models.Job, execution *models.ExecutionContext) error,
) (bool, error) {
	if len(f.pending) == 0 {
		f.onEmpty()

		return false, nil
	}

	next := f.pending[0]
	f.pending = f.pending[1:]

	if err := process(ctx, next.job, next.execution); err != nil {
		f.errs = append(f.errs, err)

		return true, err
	}

	f.done = append(f.done, next.execution)

	return true, nil
}

type fakeWorkflows struct {
	workflows map[string]*models.Workflow
	err       error
}

func (f fakeWorkflows) GetByID(_ context.Context, id string) (*models.Workflow, error) {
	return f.workflows[id], f.err
}

func greetWorkflow() *models.Workflow {
	return &models.Workflow{
		ID:        "wf",
		Name:      "greet",
		Variables: map[string]any{"team": "Vendas"},
		Nodes: []*models.WorkflowNode{
			{ID: "trigger", Type: "trigger:webhook", Category: models.CategoryTypeTrigger, Config: map[string]any{"webhook_path": "/hook"}},
			{ID: "greet", Type: "log", Config: map[string]any{"message": "hello {{.node_results.trigger.body.name}} from {{.variables.team}}"}},
		},
		Connections: []*models.Connection{{ID: "c1", SourcePort: "trigger:success", TargetPort: "greet:main"}},
	}
}

func job(executionID, workflowID string) pending {
	data := map[string]any{"body": map[string]any{"name": "Ana"}}
	activation := engine.TriggerActivation("trigger", data)

	return pending{
		job: models.Job{ID: "job-" + executionID, ExecutionID: executionID, NodeID: activation.NodeID, Port: activation.Port, Data: activation.Data},
		execution: &models.ExecutionContext{
			ID:          executionID,
			WorkflowID:  workflowID,
			Status:      models.ExecutionStatusRunning,
			TriggerData: data,
		},
	}
}

func newWorker(jobs *fakeJobs, workflows fakeWorkflows, logs io.Writer) *worker.Worker {
	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()

	return worker.New(jobs, workflows, engine.New(reg), slog.New(slog.NewTextHandler(logs, nil)), time.Millisecond)
}

func runUntilEmpty(t *testing.T, jobs *fakeJobs, workflows fakeWorkflows, logs io.Writer) error {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	jobs.onEmpty = cancel

	return newWorker(jobs, workflows, logs).Run(ctx)
}

func TestRunProcessesJobsUntilCancelled(t *testing.T) {
	jobs := &fakeJobs{pending: []pending{job("exec-1", "wf"), job("exec-2", "wf")}}
	workflows := fakeWorkflows{workflows: map[string]*models.Workflow{"wf": greetWorkflow()}}

	err := runUntilEmpty(t, jobs, workflows, io.Discard)

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, jobs.done, 2)

	for _, execution := range jobs.done {
		assert.Equal(t, models.ExecutionStatusCompleted, execution.Status)
		assert.Equal(t, "hello Ana from Vendas", execution.NodeResults["greet"].Data["message"])
	}
}

func TestRunFailsExecutionWhenWorkflowIsMissing(t *testing.T) {
	jobs := &fakeJobs{pending: []pending{job("exec-1", "missing")}}

	err := runUntilEmpty(t, jobs, fakeWorkflows{}, io.Discard)

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, jobs.done, 1)
	assert.Equal(t, models.ExecutionStatusFailed, jobs.done[0].Status)
	assert.Equal(t, `workflow "missing" not found`, jobs.done[0].ErrorMessage)
	assert.NotNil(t, jobs.done[0].CompletedAt)
}

func TestRunKeepsJobWhenWorkflowLookupFails(t *testing.T) {
	lookupErr := errors.New("connection reset")
	jobs := &fakeJobs{pending: []pending{job("exec-1", "wf")}}

	var logs bytes.Buffer

	err := runUntilEmpty(t, jobs, fakeWorkflows{err: lookupErr}, &logs)

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, jobs.done)
	require.Len(t, jobs.errs, 1)
	require.ErrorIs(t, jobs.errs[0], lookupErr)
	assert.Contains(t, logs.String(), "failed to process job")
}

func TestRunKeepsJobWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	jobs := &fakeJobs{pending: []pending{job("exec-1", "wf")}, onEmpty: func() {}}
	workflows := fakeWorkflows{workflows: map[string]*models.Workflow{"wf": greetWorkflow()}}

	var logs bytes.Buffer

	err := newWorker(jobs, workflows, &logs).Run(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, jobs.done)
	require.Len(t, jobs.errs, 1)
	require.ErrorIs(t, jobs.errs[0], context.Canceled)
	assert.NotContains(t, logs.String(), "level=ERROR")
}
