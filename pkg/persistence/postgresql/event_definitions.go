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

type EventDefinitions struct {
	db *sql.DB
}

func (p *Persistence) EventDefinitions() *EventDefinitions {
	return &EventDefinitions{db: p.db}
}

const eventDefinitionColumns = `key, name, description, topic, format, schema, tenant_from, tenant_key, created_at, updated_at`

func (e *EventDefinitions) Save(ctx context.Context, definition *models.EventDefinition) error {
	schema, err := json.Marshal(definition.Schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema of %s: %w", definition.Key, err)
	}

	now := time.Now().UTC()

	err = e.db.QueryRowContext(ctx, `
		INSERT INTO event_definitions (`+eventDefinitionColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
		ON CONFLICT (key) DO UPDATE SET
			name = EXCLUDED.name,
			description = EXCLUDED.description,
			topic = EXCLUDED.topic,
			format = EXCLUDED.format,
			schema = EXCLUDED.schema,
			tenant_from = EXCLUDED.tenant_from,
			tenant_key = EXCLUDED.tenant_key,
			updated_at = EXCLUDED.updated_at
		RETURNING created_at, updated_at
	`, definition.Key, definition.Name, definition.Description, definition.Topic, definition.Format, schema,
		definition.TenantFrom, definition.TenantKey, now,
	).Scan(&definition.CreatedAt, &definition.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to save event definition %s: %w", definition.Key, err)
	}

	return nil
}

func (e *EventDefinitions) Get(ctx context.Context, key string) (*models.EventDefinition, error) {
	definition, err := scanEventDefinition(e.db.QueryRowContext(ctx, `SELECT `+eventDefinitionColumns+` FROM event_definitions WHERE key = $1`, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", persistence.ErrEventDefinitionNotFound, key)
	}

	return definition, err
}

func (e *EventDefinitions) List(ctx context.Context) ([]*models.EventDefinition, error) {
	return e.query(ctx, `SELECT `+eventDefinitionColumns+` FROM event_definitions ORDER BY name, key`)
}

func (e *EventDefinitions) ByTopic(ctx context.Context, topic string) ([]*models.EventDefinition, error) {
	return e.query(ctx, `SELECT `+eventDefinitionColumns+` FROM event_definitions WHERE topic = $1 ORDER BY key`, topic)
}

func (e *EventDefinitions) query(ctx context.Context, query string, args ...any) ([]*models.EventDefinition, error) {
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list event definitions: %w", err)
	}

	defer func() { _ = rows.Close() }()

	definitions := []*models.EventDefinition{}

	for rows.Next() {
		definition, err := scanEventDefinition(rows)
		if err != nil {
			return nil, err
		}

		definitions = append(definitions, definition)
	}

	return definitions, rows.Err()
}

func scanEventDefinition(scanner interface{ Scan(dest ...any) error }) (*models.EventDefinition, error) {
	var (
		definition models.EventDefinition
		schema     []byte
	)

	err := scanner.Scan(&definition.Key, &definition.Name, &definition.Description, &definition.Topic, &definition.Format,
		&schema, &definition.TenantFrom, &definition.TenantKey, &definition.CreatedAt, &definition.UpdatedAt)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(schema, &definition.Schema); err != nil {
		return nil, fmt.Errorf("failed to read schema of %s: %w", definition.Key, err)
	}

	return &definition, nil
}
