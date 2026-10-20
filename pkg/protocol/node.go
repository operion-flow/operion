// Package protocol defines the interfaces and contracts for pluggable nodes.
package protocol

import (
	"context"
	"time"

	"github.com/dukex/operion/pkg/models"
)

// Node represents an executable unit in a workflow graph.
type Node interface {
	// ID returns the unique identifier of this node instance.
	ID() string

	// Type returns the node type identifier (e.g., "log", "httprequest", "merge").
	Type() string

	// Execute processes inputs and returns outputs for this node.
	Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error)

	// InputPorts returns the input ports available on this node.
	InputPorts() []models.InputPort

	// OutputPorts returns the output ports available on this node.
	OutputPorts() []models.OutputPort

	// Validate checks if the provided configuration is valid for this node.
	Validate(config map[string]any) error
}

// NodeFactory creates node instances and provides metadata about the node type.
type NodeFactory interface {
	// Create creates a new node instance with the given configuration
	Create(ctx context.Context, id string, config map[string]any) (Node, error)

	// ID returns the unique identifier for this node type
	ID() string

	// Name returns the human-readable name for this node type
	Name() string

	// Description returns a description of what this node does
	Description() string

	// Schema returns the JSON schema for configuring this node
	Schema() map[string]any
}

// Waiter is implemented by nodes that hold their branch until a point in time.
// The engine calls WaitUntil instead of Execute on the first activation and
// calls Execute when the branch resumes.
type Waiter interface {
	WaitUntil(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (time.Time, error)
}
