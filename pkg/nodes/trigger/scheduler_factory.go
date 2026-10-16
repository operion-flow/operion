package trigger

import (
	"context"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
)

// SchedulerTriggerNodeFactory creates SchedulerTriggerNode instances.
type SchedulerTriggerNodeFactory struct{}

// NewSchedulerTriggerNodeFactory creates a new scheduler trigger node factory.
func NewSchedulerTriggerNodeFactory() protocol.NodeFactory {
	return &SchedulerTriggerNodeFactory{}
}

// Create creates a new SchedulerTriggerNode instance.
func (f *SchedulerTriggerNodeFactory) Create(ctx context.Context, id string, config map[string]any) (protocol.Node, error) {
	return NewSchedulerTriggerNode(id, config)
}

// ID returns the factory ID.
func (f *SchedulerTriggerNodeFactory) ID() string {
	return models.NodeTypeTriggerScheduler
}

// Name returns the factory name.
func (f *SchedulerTriggerNodeFactory) Name() string {
	return "Scheduler Trigger"
}

// Description returns the factory description.
func (f *SchedulerTriggerNodeFactory) Description() string {
	return "Receives scheduled events based on cron expressions and starts workflow execution"
}

// Schema returns the JSON schema for scheduler trigger node configuration.
func (f *SchedulerTriggerNodeFactory) Schema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"cron_expression": map[string]any{
				"type":        "string",
				"description": "Cron expression defining when the scheduler should trigger",
				"examples": []string{
					"0 9 * * MON-FRI", // Every weekday at 9 AM
					"0 0 1 * *",       // First day of every month at midnight
					"*/15 * * * *",    // Every 15 minutes
					"0 2 * * SUN",     // Every Sunday at 2 AM
					"0 0,12 * * *",    // Twice a day at midnight and noon
				},
			},
			"timezone": map[string]any{
				"type":        "string",
				"description": "Timezone for the cron expression",
				"default":     "UTC",
				"examples": []string{
					"UTC",
					"America/New_York",
					"Europe/London",
					"Asia/Tokyo",
				},
			},
		},
		schemaRequired: []string{"cron_expression"},
		"examples": []map[string]any{
			{
				"cron_expression": "0 9 * * MON-FRI",
				"timezone":        "America/New_York",
			},
			{
				"cron_expression": "*/30 * * * *",
				"timezone":        "UTC",
			},
		},
	}
}
