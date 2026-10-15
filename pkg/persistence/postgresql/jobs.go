package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dukex/operion/pkg/models"
)

type Jobs struct {
	db         *sql.DB
	executions *ExecutionContextRepository
}

func (p *Persistence) Jobs() *Jobs {
	return &Jobs{db: p.db, executions: p.executionContextRepo}
}

func (j *Jobs) Start(ctx context.Context, execution *models.ExecutionContext, job models.Job) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	columns, err := executionColumns(execution)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO execution_contexts (
			id, workflow_id, status, node_results, variables,
			trigger_data, metadata, error_message, created_at, completed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, columns...)
	if err != nil {
		return fmt.Errorf("failed to insert execution: %w", err)
	}

	data, err := json.Marshal(job.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal job data: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (id, execution_id, node_id, port, data, run_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, job.ID, job.ExecutionID, job.NodeID, job.Port, data, job.RunAt)
	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	return tx.Commit()
}

func (j *Jobs) ProcessNext(
	ctx context.Context,
	process func(ctx context.Context, job models.Job, execution *models.ExecutionContext) error,
) (bool, error) {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	var (
		job  models.Job
		data []byte
	)

	err = tx.QueryRowContext(ctx, `
		SELECT id, execution_id, node_id, port, data, run_at
		FROM jobs
		WHERE run_at <= NOW()
		ORDER BY run_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&job.ID, &job.ExecutionID, &job.NodeID, &job.Port, &data, &job.RunAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to claim job: %w", err)
	}

	if err := json.Unmarshal(data, &job.Data); err != nil {
		return true, fmt.Errorf("failed to unmarshal job %s data: %w", job.ID, err)
	}

	execution, err := j.executions.scanExecutionContext(tx.QueryRowContext(ctx, `
		SELECT id, workflow_id, status, node_results, variables,
			   trigger_data, metadata, error_message, created_at, completed_at
		FROM execution_contexts
		WHERE id = $1
		FOR UPDATE
	`, job.ExecutionID))
	if err != nil {
		return true, fmt.Errorf("failed to load execution %s: %w", job.ExecutionID, err)
	}

	if err := process(ctx, job, execution); err != nil {
		return true, err
	}

	nodeResults, err := json.Marshal(execution.NodeResults)
	if err != nil {
		return true, fmt.Errorf("failed to marshal node results: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE execution_contexts
		SET status = $2, node_results = $3, error_message = $4, completed_at = $5
		WHERE id = $1
	`, execution.ID, execution.Status, nodeResults, execution.ErrorMessage, execution.CompletedAt)
	if err != nil {
		return true, fmt.Errorf("failed to update execution %s: %w", execution.ID, err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = $1`, job.ID); err != nil {
		return true, fmt.Errorf("failed to delete job %s: %w", job.ID, err)
	}

	return true, tx.Commit()
}

func executionColumns(execution *models.ExecutionContext) ([]any, error) {
	nodeResults, nodeResultsErr := json.Marshal(execution.NodeResults)
	variables, variablesErr := json.Marshal(execution.Variables)
	triggerData, triggerDataErr := json.Marshal(execution.TriggerData)
	metadata, metadataErr := json.Marshal(execution.Metadata)

	if err := errors.Join(nodeResultsErr, variablesErr, triggerDataErr, metadataErr); err != nil {
		return nil, fmt.Errorf("failed to marshal execution %s: %w", execution.ID, err)
	}

	return []any{
		execution.ID, execution.WorkflowID, execution.Status, nodeResults, variables,
		triggerData, metadata, execution.ErrorMessage, execution.CreatedAt, execution.CompletedAt,
	}, nil
}
