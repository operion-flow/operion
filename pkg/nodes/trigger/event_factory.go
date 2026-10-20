package trigger

import (
	"context"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
)

type EventTriggerNodeFactory struct{}

func NewEventTriggerNodeFactory() protocol.NodeFactory {
	return &EventTriggerNodeFactory{}
}

func (f *EventTriggerNodeFactory) Create(_ context.Context, id string, config map[string]any) (protocol.Node, error) {
	return NewEventTriggerNode(id, config)
}

func (f *EventTriggerNodeFactory) ID() string {
	return models.NodeTypeTriggerEvent
}

func (f *EventTriggerNodeFactory) Name() string {
	return "Event Trigger"
}

func (f *EventTriggerNodeFactory) Description() string {
	return "Starts the workflow when the platform publishes a catalog event for the workflow's tenant"
}

func (f *EventTriggerNodeFactory) Schema() map[string]any {
	return map[string]any{schemaRequired: []string{eventConfigKey}}
}
