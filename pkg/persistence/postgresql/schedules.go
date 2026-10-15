package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dukex/operion/pkg/models"
)

type Schedules struct {
	db *sql.DB
}

func (p *Persistence) Schedules() *Schedules {
	return &Schedules{db: p.db}
}

func (s *Schedules) Sync(ctx context.Context, first func(models.Schedule) (time.Time, error)) error {
	missing, err := s.missing(ctx)
	if err != nil {
		return err
	}

	var errs []error

	for _, schedule := range missing {
		nextRunAt, err := first(schedule)
		if err != nil {
			errs = append(errs, fmt.Errorf("workflow %s node %q: %w", schedule.WorkflowID, schedule.NodeID, err))

			continue
		}

		_, err = s.db.ExecContext(ctx, `
			INSERT INTO schedules (workflow_id, node_id, next_run_at)
			VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING
		`, schedule.WorkflowID, schedule.NodeID, nextRunAt)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to insert schedule for workflow %s: %w", schedule.WorkflowID, err))
		}
	}

	return errors.Join(errs...)
}

func (s *Schedules) missing(ctx context.Context) ([]models.Schedule, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT n.workflow_id, n.id, n.config->>'cron_expression', COALESCE(NULLIF(n.config->>'timezone', ''), 'UTC')
		FROM workflow_nodes n
		JOIN workflows w ON w.id = n.workflow_id
		WHERE n.type = 'trigger:scheduler'
		  AND w.status = $1
		  AND w.deleted_at IS NULL
		  AND NOT EXISTS (SELECT 1 FROM schedules s WHERE s.workflow_id = n.workflow_id AND s.node_id = n.id)
	`, models.WorkflowStatusPublished)
	if err != nil {
		return nil, fmt.Errorf("failed to query scheduler triggers: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var missing []models.Schedule

	for rows.Next() {
		var schedule models.Schedule
		if err := rows.Scan(&schedule.WorkflowID, &schedule.NodeID, &schedule.Expression, &schedule.Timezone); err != nil {
			return nil, fmt.Errorf("failed to scan scheduler trigger: %w", err)
		}

		missing = append(missing, schedule)
	}

	return missing, rows.Err()
}

func (s *Schedules) ProcessDue(ctx context.Context, fire func(context.Context, models.Schedule) (models.Firing, error)) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	var schedule models.Schedule

	err = tx.QueryRowContext(ctx, `
		SELECT s.workflow_id, s.node_id, s.next_run_at, n.config->>'cron_expression', COALESCE(NULLIF(n.config->>'timezone', ''), 'UTC')
		FROM schedules s
		JOIN workflow_nodes n ON n.workflow_id = s.workflow_id AND n.id = s.node_id
		JOIN workflows w ON w.id = s.workflow_id
		WHERE s.next_run_at <= NOW()
		  AND w.status = $1
		  AND w.deleted_at IS NULL
		ORDER BY s.next_run_at
		LIMIT 1
		FOR UPDATE OF s SKIP LOCKED
	`, models.WorkflowStatusPublished).Scan(&schedule.WorkflowID, &schedule.NodeID, &schedule.NextRunAt, &schedule.Expression, &schedule.Timezone)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to claim schedule: %w", err)
	}

	firing, err := fire(ctx, schedule)
	if err != nil {
		return true, err
	}

	if err := insertStart(ctx, tx, firing.Execution, firing.Job); err != nil {
		return true, err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE schedules SET next_run_at = $3 WHERE workflow_id = $1 AND node_id = $2
	`, schedule.WorkflowID, schedule.NodeID, firing.NextRunAt)
	if err != nil {
		return true, fmt.Errorf("failed to move schedule of workflow %s: %w", schedule.WorkflowID, err)
	}

	return true, tx.Commit()
}
