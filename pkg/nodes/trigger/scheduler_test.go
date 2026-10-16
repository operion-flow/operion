package trigger_test

import (
	"testing"

	"github.com/dukex/operion/pkg/nodes/trigger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cronKey = "cron_expression"

func TestSchedulerTriggerValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]any
		message string
	}{
		{name: "valid", config: map[string]any{cronKey: "0 9 * * 1-5", "timezone": "America/Sao_Paulo"}},
		{name: "missing expression", config: map[string]any{}, message: "cron_expression is required"},
		{name: "invalid expression", config: map[string]any{cronKey: "every monday"}, message: "invalid cron_expression"},
		{name: "invalid timezone", config: map[string]any{cronKey: "0 9 * * *", "timezone": "Mars/Olympus"}, message: "invalid timezone"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := trigger.NewSchedulerTriggerNode("schedule", map[string]any{cronKey: "* * * * *"})
			require.NoError(t, err)

			err = node.Validate(tt.config)

			if tt.message == "" {
				require.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.message)
			}
		})
	}
}
