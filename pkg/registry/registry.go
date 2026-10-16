package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/dukex/operion/pkg/protocol"
)

// ErrNodeNotRegistered is returned when a node type is not registered.
var ErrNodeNotRegistered = errors.New("node type not registered")

type Registry struct {
	nodeFactories map[string]protocol.NodeFactory
}

func NewRegistry() *Registry {
	return &Registry{nodeFactories: make(map[string]protocol.NodeFactory)}
}

func (r *Registry) RegisterNode(nodeFactory protocol.NodeFactory) {
	r.nodeFactories[nodeFactory.ID()] = nodeFactory
}

// CreateNode creates a new node instance based on the provided node type and configuration.
func (r *Registry) CreateNode(
	ctx context.Context,
	nodeType string,
	nodeID string,
	config map[string]any,
) (protocol.Node, error) {
	factory, ok := r.nodeFactories[nodeType]
	if !ok {
		return nil, fmt.Errorf("node type '%s': %w", nodeType, ErrNodeNotRegistered)
	}

	created, err := factory.Create(ctx, nodeID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create node '%s': %w", nodeType, err)
	}

	return created, nil
}
