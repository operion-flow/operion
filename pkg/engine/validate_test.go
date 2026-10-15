package engine_test

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateAcceptsRunnableWorkflow(t *testing.T) {
	wf := workflow(
		[]*models.WorkflowNode{
			trigger(),
			node("check", "conditional", map[string]any{"condition": "{{.node_results.trigger.body.active}}"}),
			logNode("active"),
			logNode("inactive"),
		},
		connect("trigger:success", "check:main"),
		connect("check:true", "active:main"),
		connect("check:false", "inactive:main"),
	)

	require.NoError(t, newEngine(nil).Validate(t.Context(), wf))
}

func TestValidateRejectsInvalidWorkflow(t *testing.T) {
	tests := []struct {
		name     string
		workflow *models.Workflow
		messages []string
	}{
		{
			name: "unknown node type",
			workflow: workflow(
				[]*models.WorkflowNode{trigger(), node("step", "missing", nil)},
				connect("trigger:success", "step:main"),
			),
			messages: []string{`node "step": `, registry.ErrNodeNotRegistered.Error()},
		},
		{
			name: "config rejected by the node",
			workflow: workflow(
				[]*models.WorkflowNode{trigger(), node("step", "log", map[string]any{"message": "hi", "level": "loud"})},
				connect("trigger:success", "step:main"),
			),
			messages: []string{`node "step": `, "loud"},
		},
		{
			name: "undeclared ports",
			workflow: workflow(
				[]*models.WorkflowNode{trigger(), logNode("done")},
				connect("trigger:output", "done:input"),
			),
			messages: []string{
				`connection "trigger:output->done:input": output port "trigger:output" does not exist`,
				`connection "trigger:output->done:input": input port "done:input" does not exist`,
			},
		},
		{
			name: "cycle",
			workflow: workflow(
				[]*models.WorkflowNode{trigger(), logNode("first"), logNode("second")},
				connect("trigger:success", "first:main"),
				connect("first:success", "second:main"),
				connect("second:success", "first:main"),
			),
			messages: []string{`node "first" is part of a cycle`},
		},
		{
			name: "unreachable node",
			workflow: workflow(
				[]*models.WorkflowNode{trigger(), logNode("orphan")},
			),
			messages: []string{`node "orphan" is not reachable from a trigger`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newEngine(nil).Validate(t.Context(), tt.workflow)

			require.Error(t, err)

			for _, message := range tt.messages {
				assert.Contains(t, err.Error(), message)
			}
		})
	}
}
