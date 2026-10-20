package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/go-playground/validator/v10"
)

var validate = validator.New(validator.WithRequiredStructEnabled())

// Prepare validates a node type the way the platform API accepts it and fills its defaults.
func Prepare(nodeType *models.RemoteNodeType) error {
	if err := validate.Struct(nodeType); err != nil {
		return err
	}

	Normalize(nodeType)

	return nil
}

// Types holds node types in memory by id.
type Types map[string]models.RemoteNodeType

// ReadTypes reads a JSON object of node types keyed by id, each with the body of PUT /platform/node-types/:id, and prepares every one.
func ReadTypes(path string) (Types, error) {
	content, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}

	var types Types
	if err := json.Unmarshal(content, &types); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	for _, id := range slices.Sorted(maps.Keys(types)) {
		nodeType := types[id]
		nodeType.ID = id

		if err := Prepare(&nodeType); err != nil {
			return nil, fmt.Errorf("%s: node type %q: %w", path, id, err)
		}

		types[id] = nodeType
	}

	return types, nil
}

// Get returns the node type with the given id.
func (t Types) Get(_ context.Context, id string) (*models.RemoteNodeType, error) {
	nodeType, ok := t[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", persistence.ErrRemoteNodeTypeNotFound, id)
	}

	return &nodeType, nil
}
