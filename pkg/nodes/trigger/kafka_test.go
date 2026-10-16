package trigger_test

import (
	"testing"

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
