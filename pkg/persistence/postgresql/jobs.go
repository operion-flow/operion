package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
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

	if err := insertStart(ctx, tx, execution, job); err != nil {
		return err
	}

	return tx.Commit()
}

func insertStart(ctx context.Context, tx *sql.Tx, execution *models.ExecutionContext, job models.Job) error {
	columns, err := executionColumns(execution)
	if err != nil {
		return err
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO execution_contexts (
			id, workflow_id, status, node_results, variables,
			trigger_data, metadata, error_message, created_at, completed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO NOTHING
	`, columns...)
	if err != nil {
		return fmt.Errorf("failed to insert execution: %w", err)
	}

	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to insert execution: %w", err)
	}

	if inserted == 0 {
		return nil
	}

	return insertJob(ctx, tx, job)
}

func insertJob(ctx context.Context, tx *sql.Tx, job models.Job) error {
	data, err := json.Marshal(job.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal job data: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (id, execution_id, node_id, port, data, run_at, attempt)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, job.ID, job.ExecutionID, job.NodeID, job.Port, data, job.RunAt, job.Attempt)
	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	return nil
}

func (j *Jobs) Resume(ctx context.Context, executionID, nodeID string, data map[string]any) error {
	return j.ResumeIf(ctx, executionID, nodeID, data, func(models.NodeResult) bool { return true })
}

func (j *Jobs) ResumeIf(ctx context.Context, executionID, nodeID string, data map[string]any, allowed func(models.NodeResult) bool) error {
	var encoded any

	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal resume data: %w", err)
		}

		encoded = string(raw)
	}

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	var raw []byte

	err = tx.QueryRowContext(ctx, `SELECT node_results FROM execution_contexts WHERE id = $1 FOR UPDATE`, executionID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", persistence.ErrExecutionNotFound, executionID)
	}

	if err != nil {
		return fmt.Errorf("failed to lock execution %s: %w", executionID, err)
	}

	var results map[string]models.NodeResult
	if err := json.Unmarshal(raw, &results); err != nil {
		return fmt.Errorf("failed to read node results of execution %s: %w", executionID, err)
	}

	if !allowed(results[nodeID]) {
		return fmt.Errorf("%w: %s in execution %s", persistence.ErrResumeRefused, nodeID, executionID)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE jobs SET run_at = NOW(), data = COALESCE($4::jsonb, data)
		WHERE execution_id = $1 AND node_id = $2 AND port = $3 AND run_at > NOW()
	`, executionID, nodeID, engine.ResumePort, encoded)
	if err != nil {
		return fmt.Errorf("failed to resume node %s of execution %s: %w", nodeID, executionID, err)
	}

	resumed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to resume node %s of execution %s: %w", nodeID, executionID, err)
	}

	if resumed == 0 {
		return fmt.Errorf("%w: %s in execution %s", persistence.ErrNodeNotWaiting, nodeID, executionID)
	}

	return tx.Commit()
}

func (j *Jobs) ProcessNext(
	ctx context.Context,
	process func(ctx context.Context, job models.Job, execution *models.ExecutionContext) ([]models.Job, error),
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
		SELECT id, execution_id, node_id, port, data, run_at, attempt
		FROM jobs
		WHERE run_at <= NOW()
		ORDER BY run_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&job.ID, &job.ExecutionID, &job.NodeID, &job.Port, &data, &job.RunAt, &job.Attempt)
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

	next, err := process(ctx, job, execution)
	if err != nil {
		return true, err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = $1`, job.ID); err != nil {
		return true, fmt.Errorf("failed to delete job %s: %w", job.ID, err)
	}

	for _, later := range next {
		if err := insertJob(ctx, tx, later); err != nil {
			return true, err
		}
	}

	if err := settle(ctx, tx, execution); err != nil {
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

	return true, tx.Commit()
}

func settle(ctx context.Context, tx *sql.Tx, execution *models.ExecutionContext) error {
	if execution.Status == models.ExecutionStatusFailed {
		_, err := tx.ExecContext(ctx, `
			DELETE FROM jobs WHERE id IN (
				SELECT id FROM jobs WHERE execution_id = $1 FOR UPDATE SKIP LOCKED
			)
		`, execution.ID)
		if err != nil {
			return fmt.Errorf("failed to drop jobs of failed execution %s: %w", execution.ID, err)
		}

		return nil
	}

	if execution.Status != models.ExecutionStatusCompleted {
		return nil
	}

	var pending bool

	err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE execution_id = $1)`, execution.ID).Scan(&pending)
	if err != nil {
		return fmt.Errorf("failed to check pending jobs of execution %s: %w", execution.ID, err)
	}

	if pending {
		execution.Status = models.ExecutionStatusRunning
		execution.CompletedAt = nil
	}

	return nil
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
