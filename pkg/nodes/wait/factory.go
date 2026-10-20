package wait

import (
	"context"

	"github.com/dukex/operion/pkg/protocol"
)

type Factory struct {
	nodeType string
}

func NewWaitFactory() protocol.NodeFactory {
	return &Factory{nodeType: TypeWait}
}

func NewWaitUntilFactory() protocol.NodeFactory {
	return &Factory{nodeType: TypeWaitUntil}
}

func (f *Factory) Create(_ context.Context, id string, config map[string]any) (protocol.Node, error) {
	if f.nodeType == TypeWait {
		return NewWait(id, config)
	}

	return NewWaitUntil(id, config)
}

func (f *Factory) ID() string {
	return f.nodeType
}

func (f *Factory) Name() string {
	if f.nodeType == TypeWait {
		return "Wait"
	}

	return "Wait until"
}

func (f *Factory) Description() string {
	if f.nodeType == TypeWait {
		return "Holds the branch for a duration, then continues with the same data"
	}

	return "Holds the branch until a time taken from the event or earlier nodes, then continues with the same data"
}

func (f *Factory) Schema() map[string]any {
	if f.nodeType == TypeWait {
		return schema(ConfigDuration, "How long to wait, as a Go duration", "15m", "72h")
	}

	return schema(ConfigUntil, "Template that renders the RFC 3339 time to continue at", "{{.node_results.trigger.message.start_date}}", "2026-12-01T09:00:00-03:00")
}

func schema(field, description string, examples ...string) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			field: map[string]any{"type": "string", "description": description, "examples": examples},
		},
		"required": []string{field},
	}
}
