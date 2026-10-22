package trigger_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/trigger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaTriggerOnlyNeedsATopic(t *testing.T) {
	node, err := trigger.NewKafkaTriggerNode("employees", map[string]any{"topic": "employees"})
	require.NoError(t, err)

	require.NoError(t, node.Validate(map[string]any{"topic": "employees"}))
	assert.ErrorContains(t, node.Validate(map[string]any{}), "topic is required")

	_, err = trigger.NewKafkaTriggerNode("employees", map[string]any{})
	assert.ErrorContains(t, err, "topic is required")
}

func TestKafkaTriggerOnlyReadsItsTopic(t *testing.T) {
	factory := trigger.NewKafkaTriggerNodeFactory()

	properties, ok := factory.Schema()["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []string{"topic"}, slices.Sorted(maps.Keys(properties)))

	node, err := factory.Create(t.Context(), "audit", map[string]any{"topic": "tp_audit", "consumer_group": "audit", "brokers": []any{"kafka:9092"}})
	require.NoError(t, err)

	outputs, err := node.Execute(models.ExecutionContext{}, map[string]models.NodeResult{
		trigger.KafkaInputPortExternal: {Data: map[string]any{"topic": "tp_audit", "message_data": map[string]any{"action": "login"}}},
	})
	require.NoError(t, err)

	sent := outputs[trigger.KafkaOutputPortSuccess].Data
	assert.Equal(t, "tp_audit", sent["topic"])
	assert.Equal(t, map[string]any{"action": "login"}, sent["message"])
	assert.NotContains(t, sent, "config")
}
