package trigger

import (
	"errors"

	"github.com/dukex/operion/pkg/models"
)

const (
	EventInputPortExternal = "external"
	EventOutputPortSuccess = "success"
	eventConfigKey         = "event"
)

var (
	errEventRequired      = errors.New("event is required and must be a non-empty string")
	errEventInputNotFound = errors.New("external input not found")
)

type EventTriggerNode struct {
	id string
}

func NewEventTriggerNode(id string, config map[string]any) (*EventTriggerNode, error) {
	node := &EventTriggerNode{id: id}
	if err := node.Validate(config); err != nil {
		return nil, err
	}

	return node, nil
}

func (n *EventTriggerNode) ID() string {
	return n.id
}

func (n *EventTriggerNode) Type() string {
	return models.NodeTypeTriggerEvent
}

func (n *EventTriggerNode) Execute(_ models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	external, ok := inputs[EventInputPortExternal]
	if !ok {
		return nil, errEventInputNotFound
	}

	return map[string]models.NodeResult{
		EventOutputPortSuccess: {NodeID: n.id, Data: external.Data, Status: string(models.NodeStatusSuccess)},
	}, nil
}

func (n *EventTriggerNode) InputPorts() []models.InputPort {
	return []models.InputPort{{Port: n.port(EventInputPortExternal, "Catalog event received from the event bus")}}
}

func (n *EventTriggerNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{{Port: n.port(EventOutputPortSuccess, "The event that started the execution")}}
}

func (n *EventTriggerNode) Validate(config map[string]any) error {
	if event, ok := config[eventConfigKey].(string); !ok || event == "" {
		return errEventRequired
	}

	return nil
}

func (n *EventTriggerNode) port(name, description string) models.Port {
	return models.Port{
		ID:          models.MakePortID(n.id, name),
		NodeID:      n.id,
		Name:        name,
		Description: description,
	}
}
