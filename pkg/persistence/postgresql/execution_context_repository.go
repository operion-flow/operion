package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
)

// ExecutionContextRepository handles execution context-related database operations.
type ExecutionContextRepository struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewExecutionContextRepository creates a new execution context repository.
func NewExecutionContextRepository(db *sql.DB, logger *slog.Logger) *ExecutionContextRepository {
	return &ExecutionContextRepository{db: db, logger: logger}
}

// SaveExecutionContext saves an execution context to the database.
func (ecr *ExecutionContextRepository) SaveExecutionContext(ctx context.Context, execCtx *models.ExecutionContext) error {
	// Marshal complex fields to JSON
	nodeResultsJSON, err := json.Marshal(execCtx.NodeResults)
	if err != nil {
		return fmt.Errorf("failed to marshal node results: %w", err)
	}

	variablesJSON, err := json.Marshal(execCtx.Variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables: %w", err)
	}

	triggerDataJSON, err := json.Marshal(execCtx.TriggerData)
	if err != nil {
		return fmt.Errorf("failed to marshal trigger data: %w", err)
	}

	metadataJSON, err := json.Marshal(execCtx.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		INSERT INTO execution_contexts (
			id, workflow_id, status, node_results, variables, 
			trigger_data, metadata, error_message, created_at, completed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			workflow_id = EXCLUDED.workflow_id,
			status = EXCLUDED.status,
			node_results = EXCLUDED.node_results,
			variables = EXCLUDED.variables,
			trigger_data = EXCLUDED.trigger_data,
			metadata = EXCLUDED.metadata,
			error_message = EXCLUDED.error_message,
			completed_at = EXCLUDED.completed_at
	`

	_, err = ecr.db.ExecContext(ctx, query,
		execCtx.ID,
		execCtx.WorkflowID,
		execCtx.Status,
		nodeResultsJSON,
		variablesJSON,
		triggerDataJSON,
		metadataJSON,
		execCtx.ErrorMessage,
		execCtx.CreatedAt,
		execCtx.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to save execution context: %w", err)
	}

	return nil
}

// GetExecutionContext retrieves an execution context by its ID from the database.
func (ecr *ExecutionContextRepository) GetExecutionContext(ctx context.Context, executionID string) (*models.ExecutionContext, error) {
	query := `
		SELECT id, workflow_id, status, node_results, variables, 
			   trigger_data, metadata, error_message, created_at, completed_at
		FROM execution_contexts
		WHERE id = $1
	`

	row := ecr.db.QueryRowContext(ctx, query, executionID)

	execCtx, err := ecr.scanExecutionContext(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", persistence.ErrExecutionNotFound, executionID)
		}

		return nil, fmt.Errorf("failed to scan execution context: %w", err)
	}

	return execCtx, nil
}

// UpdateExecutionContext updates an existing execution context in the database.
func (ecr *ExecutionContextRepository) UpdateExecutionContext(ctx context.Context, execCtx *models.ExecutionContext) error {
	// Check if execution context exists first
	_, err := ecr.GetExecutionContext(ctx, execCtx.ID)
	if err != nil {
		return err
	}

	// Use SaveExecutionContext as it handles upsert logic
	return ecr.SaveExecutionContext(ctx, execCtx)
}

// GetExecutionsByWorkflow retrieves all execution contexts for a specific workflow.
func (ecr *ExecutionContextRepository) GetExecutionsByWorkflow(ctx context.Context, workflowID string, limit int) ([]*models.ExecutionContext, error) {
	query := `
		SELECT id, workflow_id, status, node_results, variables, 
			   trigger_data, metadata, error_message, created_at, completed_at
		FROM execution_contexts
		WHERE workflow_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`

	rows, err := ecr.db.QueryContext(ctx, query, workflowID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query execution contexts: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			ecr.logger.ErrorContext(ctx, "failed to close rows", "error", closeErr)
		}
	}()

	var executions []*models.ExecutionContext

	for rows.Next() {
		execCtx, err := ecr.scanExecutionContext(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan execution context: %w", err)
		}

		executions = append(executions, execCtx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating execution contexts: %w", err)
	}

	return executions, nil
}

// GetExecutionsByStatus retrieves all execution contexts with a specific status.
func (ecr *ExecutionContextRepository) GetExecutionsByStatus(ctx context.Context, status models.ExecutionStatus) ([]*models.ExecutionContext, error) {
	query := `
		SELECT id, workflow_id, status, node_results, variables, 
			   trigger_data, metadata, error_message, created_at, completed_at
		FROM execution_contexts
		WHERE status = $1
		ORDER BY created_at DESC
	`

	rows, err := ecr.db.QueryContext(ctx, query, status)
	if err != nil {
		return nil, fmt.Errorf("failed to query execution contexts: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			ecr.logger.ErrorContext(ctx, "failed to close rows", "error", closeErr)
		}
	}()

	var executions []*models.ExecutionContext

	for rows.Next() {
		execCtx, err := ecr.scanExecutionContext(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan execution context: %w", err)
		}

		executions = append(executions, execCtx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating execution contexts: %w", err)
	}

	return executions, nil
}

// scanExecutionContext scans an execution context from a database row.
func (ecr *ExecutionContextRepository) scanExecutionContext(scanner interface {
	Scan(dest ...any) error
}) (*models.ExecutionContext, error) {
	var (
		execCtx                                                       models.ExecutionContext
		nodeResultsJSON, variablesJSON, triggerDataJSON, metadataJSON []byte
	)

	err := scanner.Scan(
		&execCtx.ID,
		&execCtx.WorkflowID,
		&execCtx.Status,
		&nodeResultsJSON,
		&variablesJSON,
		&triggerDataJSON,
		&metadataJSON,
		&execCtx.ErrorMessage,
		&execCtx.CreatedAt,
		&execCtx.CompletedAt,
	)
	if err != nil {
		return nil, err
	}

	// Initialize maps to avoid nil pointer dereferences
	execCtx.NodeResults = make(map[string]models.NodeResult)
	execCtx.Variables = make(map[string]any)
	execCtx.TriggerData = make(map[string]any)
	execCtx.Metadata = make(map[string]any)

	// Unmarshal JSON fields
	if nodeResultsJSON != nil {
		err := json.Unmarshal(nodeResultsJSON, &execCtx.NodeResults)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal node results: %w", err)
		}
	}

	if variablesJSON != nil {
		err := json.Unmarshal(variablesJSON, &execCtx.Variables)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal variables: %w", err)
		}
	}

	if triggerDataJSON != nil {
		err := json.Unmarshal(triggerDataJSON, &execCtx.TriggerData)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal trigger data: %w", err)
		}
	}

	if metadataJSON != nil {
		err := json.Unmarshal(metadataJSON, &execCtx.Metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &execCtx, nil
}
