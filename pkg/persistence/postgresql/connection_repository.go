package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
)

// ConnectionRepository handles connection-related database operations.
type ConnectionRepository struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewConnectionRepository creates a new connection repository.
func NewConnectionRepository(db *sql.DB, logger *slog.Logger) *ConnectionRepository {
	return &ConnectionRepository{db: db, logger: logger}
}

// GetConnectionsBySourceNode retrieves connections from a workflow filtered by source node ID.
func (cr *ConnectionRepository) GetConnectionsBySourceNode(ctx context.Context, workflowID, sourceNodeID string) ([]*models.Connection, error) {
	query := `
		SELECT id, source_node_id, source_port, target_node_id, target_port
		FROM workflow_connections
		WHERE workflow_id = $1 AND source_node_id = $2
		ORDER BY created_at
	`

	rows, err := cr.db.QueryContext(ctx, query, workflowID, sourceNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow connections: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			cr.logger.ErrorContext(ctx, "failed to close rows", "error", closeErr)
		}
	}()

	var connections []*models.Connection

	for rows.Next() {
		connection, err := cr.scanConnection(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan connection: %w", err)
		}

		connections = append(connections, connection)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating connections: %w", err)
	}

	return connections, nil
}

// GetConnectionsByTargetNode retrieves connections from a workflow filtered by target node ID.
func (cr *ConnectionRepository) GetConnectionsByTargetNode(ctx context.Context, workflowID, targetNodeID string) ([]*models.Connection, error) {
	query := `
		SELECT id, source_node_id, source_port, target_node_id, target_port
		FROM workflow_connections
		WHERE workflow_id = $1 AND target_node_id = $2
		ORDER BY created_at
	`

	rows, err := cr.db.QueryContext(ctx, query, workflowID, targetNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow connections: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			cr.logger.ErrorContext(ctx, "failed to close rows", "error", closeErr)
		}
	}()

	var connections []*models.Connection

	for rows.Next() {
		connection, err := cr.scanConnection(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan connection: %w", err)
		}

		connections = append(connections, connection)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating connections: %w", err)
	}

	return connections, nil
}

// SaveConnection saves a connection to the database (insert or update).
func (cr *ConnectionRepository) SaveConnection(ctx context.Context, workflowID string, connection *models.Connection) error {
	// Parse port IDs to extract node IDs and port names
	sourceNodeID, sourcePortName, sourceOK := models.ParsePortID(connection.SourcePort)
	if !sourceOK {
		return persistence.ErrInvalidPortFormat
	}

	targetNodeID, targetPortName, targetOK := models.ParsePortID(connection.TargetPort)
	if !targetOK {
		return persistence.ErrInvalidPortFormat
	}

	query := `
		INSERT INTO workflow_connections (id, workflow_id, source_node_id, source_port, target_node_id, target_port, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
		ON CONFLICT (id, workflow_id) DO UPDATE SET
			source_node_id = EXCLUDED.source_node_id,
			source_port = EXCLUDED.source_port,
			target_node_id = EXCLUDED.target_node_id,
			target_port = EXCLUDED.target_port,
			updated_at = EXCLUDED.updated_at
	`

	_, err := cr.db.ExecContext(ctx, query,
		connection.ID,
		workflowID,
		sourceNodeID,
		sourcePortName,
		targetNodeID,
		targetPortName,
	)
	if err != nil {
		return fmt.Errorf("failed to save connection: %w", err)
	}

	return nil
}

// UpdateConnection updates an existing connection in the database.
func (cr *ConnectionRepository) UpdateConnection(ctx context.Context, workflowID string, connection *models.Connection) error {
	// Check if connection exists first
	_, err := cr.getConnectionByID(ctx, workflowID, connection.ID)
	if err != nil {
		return err
	}

	// Use SaveConnection as it handles upsert logic
	return cr.SaveConnection(ctx, workflowID, connection)
}

// DeleteConnection removes a connection from the database.
func (cr *ConnectionRepository) DeleteConnection(ctx context.Context, workflowID, connectionID string) error {
	query := `DELETE FROM workflow_connections WHERE workflow_id = $1 AND id = $2`

	result, err := cr.db.ExecContext(ctx, query, workflowID, connectionID)
	if err != nil {
		return fmt.Errorf("failed to delete connection: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("connection not found: %s in workflow %s", connectionID, workflowID)
	}

	return nil
}

// GetConnectionsByWorkflow retrieves all connections for a specific workflow.
func (cr *ConnectionRepository) GetConnectionsByWorkflow(ctx context.Context, workflowID string) ([]*models.Connection, error) {
	query := `
		SELECT id, source_node_id, source_port, target_node_id, target_port
		FROM workflow_connections
		WHERE workflow_id = $1
		ORDER BY created_at
	`

	rows, err := cr.db.QueryContext(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow connections: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			cr.logger.ErrorContext(ctx, "failed to close rows", "error", closeErr)
		}
	}()

	var connections []*models.Connection

	for rows.Next() {
		connection, err := cr.scanConnection(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan connection: %w", err)
		}

		connections = append(connections, connection)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating connections: %w", err)
	}

	return connections, nil
}

// getConnectionByID retrieves a specific connection by its ID from a workflow.
func (cr *ConnectionRepository) getConnectionByID(ctx context.Context, workflowID, connectionID string) (*models.Connection, error) {
	query := `
		SELECT id, source_node_id, source_port, target_node_id, target_port
		FROM workflow_connections
		WHERE workflow_id = $1 AND id = $2
	`

	row := cr.db.QueryRowContext(ctx, query, workflowID, connectionID)

	connection, err := cr.scanConnection(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("connection not found: %s in workflow %s", connectionID, workflowID)
		}

		return nil, fmt.Errorf("failed to scan connection: %w", err)
	}

	return connection, nil
}

// scanConnection scans a connection from a database row.
func (cr *ConnectionRepository) scanConnection(scanner interface {
	Scan(dest ...any) error
}) (*models.Connection, error) {
	var (
		connection                                         models.Connection
		sourceNodeID, sourcePort, targetNodeID, targetPort string
	)

	err := scanner.Scan(
		&connection.ID,
		&sourceNodeID,
		&sourcePort,
		&targetNodeID,
		&targetPort,
	)
	if err != nil {
		return nil, err
	}

	// Convert database format to new Connection struct format
	connection.SourcePort = sourceNodeID + ":" + sourcePort
	connection.TargetPort = targetNodeID + ":" + targetPort

	return &connection, nil
}
