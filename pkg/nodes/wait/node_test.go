package wait_test

import (
	"context"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/wait"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const holdID = "hold"

func execution(startDate any) models.ExecutionContext {
	return models.ExecutionContext{NodeResults: map[string]models.NodeResult{
		"trigger": {Data: map[string]any{"start_date": startDate}},
	}}
}

func TestWaitHoldsForADuration(t *testing.T) {
	node, err := wait.NewWait(holdID, map[string]any{wait.ConfigDuration: "72h"})
	require.NoError(t, err)

	before := time.Now()
	at, err := node.WaitUntil(models.ExecutionContext{}, nil)

	require.NoError(t, err)
	assert.WithinDuration(t, before.Add(72*time.Hour), at, time.Minute)
	assert.Equal(t, wait.TypeWait, node.Type())
	assert.Equal(t, holdID, node.ID())
	require.NoError(t, node.Validate(map[string]any{wait.ConfigDuration: "15m"}))
}

func TestWaitRejectsBadDurations(t *testing.T) {
	for _, config := range []map[string]any{{}, {wait.ConfigDuration: "soon"}, {wait.ConfigDuration: "-1h"}, {wait.ConfigDuration: 3}} {
		_, err := wait.NewWait(holdID, config)
		require.Error(t, err, config)
	}

	node, err := wait.NewWait(holdID, map[string]any{wait.ConfigDuration: "1h"})
	require.NoError(t, err)
	require.Error(t, node.Validate(map[string]any{wait.ConfigDuration: "0s"}))
}

func TestWaitUntilReadsTheTimeFromTheExecution(t *testing.T) {
	node, err := wait.NewWaitUntil(holdID, map[string]any{wait.ConfigUntil: "{{.node_results.trigger.start_date}}"})
	require.NoError(t, err)

	at, err := node.WaitUntil(execution("2026-12-01T09:00:00-03:00"), nil)

	require.NoError(t, err)
	assert.True(t, at.Equal(time.Date(2026, 12, 1, 12, 0, 0, 0, time.UTC)))
	assert.Equal(t, wait.TypeWaitUntil, node.Type())
	require.NoError(t, node.Validate(map[string]any{wait.ConfigUntil: "{{.trigger_data.date}}"}))
	require.Error(t, node.Validate(map[string]any{}))
}

func TestWaitUntilRejectsValuesThatAreNotTimes(t *testing.T) {
	_, err := wait.NewWaitUntil(holdID, map[string]any{})
	require.Error(t, err)

	node, err := wait.NewWaitUntil(holdID, map[string]any{wait.ConfigUntil: "{{.node_results.trigger.start_date}}"})
	require.NoError(t, err)

	_, err = node.WaitUntil(execution("tomorrow"), nil)
	require.Error(t, err)

	_, err = node.WaitUntil(execution(20261201), nil)
	require.ErrorContains(t, err, "not an RFC 3339 time")

	broken, err := wait.NewWaitUntil(holdID, map[string]any{wait.ConfigUntil: "{{.node_results"})
	require.NoError(t, err)

	_, err = broken.WaitUntil(execution(nil), nil)
	require.Error(t, err)
}

func TestWaitPassesItsInputOnWhenResumed(t *testing.T) {
	node, err := wait.NewWait(holdID, map[string]any{wait.ConfigDuration: "1h"})
	require.NoError(t, err)

	outputs, err := node.Execute(models.ExecutionContext{}, map[string]models.NodeResult{
		"resume": {Data: map[string]any{"approved": true}},
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"approved": true}, outputs[wait.OutputPortSuccess].Data)
	assert.Equal(t, string(models.NodeStatusSuccess), outputs[wait.OutputPortSuccess].Status)
	assert.Equal(t, wait.InputPortMain, node.InputPorts()[0].Name)
	assert.Equal(t, wait.OutputPortSuccess, node.OutputPorts()[0].Name)
}

func TestFactories(t *testing.T) {
	for _, tt := range []struct {
		factory interface {
			ID() string
			Name() string
			Description() string
			Schema() map[string]any
		}
		id       string
		required string
	}{
		{factory: wait.NewWaitFactory(), id: wait.TypeWait, required: wait.ConfigDuration},
		{factory: wait.NewWaitUntilFactory(), id: wait.TypeWaitUntil, required: wait.ConfigUntil},
	} {
		assert.Equal(t, tt.id, tt.factory.ID())
		assert.NotEmpty(t, tt.factory.Name())
		assert.NotEmpty(t, tt.factory.Description())
		assert.Equal(t, []string{tt.required}, tt.factory.Schema()["required"])
	}

	node, err := wait.NewWaitFactory().Create(context.Background(), holdID, map[string]any{wait.ConfigDuration: "1h"})
	require.NoError(t, err)
	assert.Equal(t, wait.TypeWait, node.Type())

	node, err = wait.NewWaitUntilFactory().Create(context.Background(), holdID, map[string]any{wait.ConfigUntil: "2026-12-01T09:00:00Z"})
	require.NoError(t, err)
	assert.Equal(t, wait.TypeWaitUntil, node.Type())
}
