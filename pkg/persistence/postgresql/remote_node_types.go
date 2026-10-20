package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
)

type RemoteNodeTypes struct {
	db *sql.DB
}

func (p *Persistence) RemoteNodeTypes() *RemoteNodeTypes {
	return &RemoteNodeTypes{db: p.db}
}

func (r *RemoteNodeTypes) Save(ctx context.Context, nodeType *models.RemoteNodeType) error {
	schema, err := json.Marshal(nodeType.Schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema of %s: %w", nodeType.ID, err)
	}

	outputs, err := json.Marshal(nodeType.Outputs)
	if err != nil {
		return fmt.Errorf("failed to marshal outputs of %s: %w", nodeType.ID, err)
	}

	err = r.db.QueryRowContext(ctx, `
		INSERT INTO remote_node_types (id, name, description, url, schema, outputs, timeout_seconds, created_at, updated_at, secret)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			description = EXCLUDED.description,
			url = EXCLUDED.url,
			secret = EXCLUDED.secret,
			schema = EXCLUDED.schema,
			outputs = EXCLUDED.outputs,
			timeout_seconds = EXCLUDED.timeout_seconds,
			updated_at = EXCLUDED.updated_at
		RETURNING created_at, updated_at
	`, nodeType.ID, nodeType.Name, nodeType.Description, nodeType.URL, schema, outputs, nodeType.TimeoutSeconds, time.Now().UTC(), nodeType.Secret,
	).Scan(&nodeType.CreatedAt, &nodeType.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to save remote node type %s: %w", nodeType.ID, err)
	}

	return nil
}

func (r *RemoteNodeTypes) Get(ctx context.Context, id string) (*models.RemoteNodeType, error) {
	var (
		nodeType        models.RemoteNodeType
		schema, outputs []byte
	)

	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, description, url, secret, schema, outputs, timeout_seconds, created_at, updated_at
		FROM remote_node_types
		WHERE id = $1
	`, id).Scan(&nodeType.ID, &nodeType.Name, &nodeType.Description, &nodeType.URL, &nodeType.Secret, &schema, &outputs,
		&nodeType.TimeoutSeconds, &nodeType.CreatedAt, &nodeType.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", persistence.ErrRemoteNodeTypeNotFound, id)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load remote node type %s: %w", id, err)
	}

	if err := errors.Join(json.Unmarshal(schema, &nodeType.Schema), json.Unmarshal(outputs, &nodeType.Outputs)); err != nil {
		return nil, fmt.Errorf("failed to read remote node type %s: %w", id, err)
	}

	return &nodeType, nil
}
