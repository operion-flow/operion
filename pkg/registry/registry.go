package registry

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/remote"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/protocol"
)

// ErrNodeNotRegistered is returned when a node type is not registered.
var ErrNodeNotRegistered = errors.New("node type not registered")

type RemoteTypes interface {
	Get(ctx context.Context, id string) (*models.RemoteNodeType, error)
}

type Registry struct {
	nodeFactories map[string]protocol.NodeFactory
	remote        RemoteTypes
	client        *http.Client
}

func NewRegistry() *Registry {
	return &Registry{nodeFactories: make(map[string]protocol.NodeFactory), client: &http.Client{}}
}

func (r *Registry) RegisterNode(nodeFactory protocol.NodeFactory) {
	r.nodeFactories[nodeFactory.ID()] = nodeFactory
}

// UseRemoteTypes makes CreateNode fall back to node types registered by the platform and served over HTTP.
func (r *Registry) UseRemoteTypes(types RemoteTypes) {
	r.remote = types
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
		return r.createRemote(ctx, nodeType, nodeID, config)
	}

	created, err := factory.Create(ctx, nodeID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create node '%s': %w", nodeType, err)
	}

	return created, nil
}

func (r *Registry) createRemote(ctx context.Context, nodeType, nodeID string, config map[string]any) (protocol.Node, error) {
	if r.remote == nil {
		return nil, fmt.Errorf("node type '%s': %w", nodeType, ErrNodeNotRegistered)
	}

	remoteType, err := r.remote.Get(ctx, nodeType)
	if errors.Is(err, persistence.ErrRemoteNodeTypeNotFound) {
		return nil, fmt.Errorf("node type '%s': %w", nodeType, ErrNodeNotRegistered)
	}

	if err != nil {
		return nil, fmt.Errorf("node type '%s': %w", nodeType, err)
	}

	return remote.New(*remoteType, nodeID, config, r.client), nil
}
