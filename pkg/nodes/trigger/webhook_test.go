package trigger_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/dukex/operion/pkg/nodes/trigger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookTriggerOnlyReadsItsPath(t *testing.T) {
	factory := trigger.NewWebhookTriggerNodeFactory()

	properties, ok := factory.Schema()["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []string{"webhook_path"}, slices.Sorted(maps.Keys(properties)))

	config := map[string]any{"webhook_path": "/orders", "method": "TRACE", "headers": map[string]any{"X-Token": "secret"}}

	node, err := factory.Create(t.Context(), "orders", config)
	require.NoError(t, err)
	require.NoError(t, node.Validate(config))
	assert.ErrorContains(t, node.Validate(map[string]any{}), "webhook_path is required")

	_, err = factory.Create(t.Context(), "orders", map[string]any{})
	assert.ErrorContains(t, err, "webhook_path is required")
}
