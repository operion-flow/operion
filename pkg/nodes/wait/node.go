package wait

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
)

const (
	InputPortMain     = "main"
	OutputPortSuccess = "success"
	TypeWait          = "wait"
	TypeWaitUntil     = "wait_until"
	ConfigDuration    = "duration"
	ConfigUntil       = "until"
)

var (
	errDurationRequired = errors.New("duration is required, as a Go duration such as 72h")
	errUntilRequired    = errors.New("until is required, as a template that renders an RFC 3339 time")
)

type Node struct {
	id       string
	nodeType string
	deadline func(ctx models.ExecutionContext) (time.Time, error)
}

func NewWait(id string, config map[string]any) (*Node, error) {
	duration, err := parseDuration(config)
	if err != nil {
		return nil, err
	}

	return &Node{id: id, nodeType: TypeWait, deadline: func(models.ExecutionContext) (time.Time, error) {
		return time.Now().Add(duration), nil
	}}, nil
}

func NewWaitUntil(id string, config map[string]any) (*Node, error) {
	until, ok := config[ConfigUntil].(string)
	if !ok || until == "" {
		return nil, errUntilRequired
	}

	return &Node{id: id, nodeType: TypeWaitUntil, deadline: func(ctx models.ExecutionContext) (time.Time, error) {
		rendered, err := template.RenderWithContext(until, &ctx)
		if err != nil {
			return time.Time{}, err
		}

		text, ok := rendered.(string)
		if !ok {
			return time.Time{}, fmt.Errorf("until rendered %v, not an RFC 3339 time", rendered)
		}

		return time.Parse(time.RFC3339, text)
	}}, nil
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) Type() string {
	return n.nodeType
}

func (n *Node) WaitUntil(ctx models.ExecutionContext, _ map[string]models.NodeResult) (time.Time, error) {
	return n.deadline(ctx)
}

func (n *Node) Execute(_ models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	var data map[string]any

	for _, port := range slices.Sorted(maps.Keys(inputs)) {
		data = inputs[port].Data
	}

	return map[string]models.NodeResult{
		OutputPortSuccess: {NodeID: n.id, Data: data, Status: string(models.NodeStatusSuccess)},
	}, nil
}

func (n *Node) InputPorts() []models.InputPort {
	return []models.InputPort{{Port: models.Port{
		ID:          models.MakePortID(n.id, InputPortMain),
		NodeID:      n.id,
		Name:        InputPortMain,
		Description: "Data to hold until the wait ends",
	}}}
}

func (n *Node) OutputPorts() []models.OutputPort {
	return []models.OutputPort{{Port: models.Port{
		ID:          models.MakePortID(n.id, OutputPortSuccess),
		NodeID:      n.id,
		Name:        OutputPortSuccess,
		Description: "The held data, or the data sent when the wait was resumed",
	}}}
}

func (n *Node) Validate(config map[string]any) error {
	if n.nodeType == TypeWait {
		_, err := parseDuration(config)

		return err
	}

	if until, ok := config[ConfigUntil].(string); !ok || until == "" {
		return errUntilRequired
	}

	return nil
}

func parseDuration(config map[string]any) (time.Duration, error) {
	raw, ok := config[ConfigDuration].(string)
	if !ok || raw == "" {
		return 0, errDurationRequired
	}

	duration, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid duration: %w", err)
	}

	if duration <= 0 {
		return 0, fmt.Errorf("duration must be positive, got %s", raw)
	}

	return duration, nil
}
