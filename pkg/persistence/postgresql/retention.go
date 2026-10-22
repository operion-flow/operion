package postgresql

import (
	"context"
	"fmt"
	"time"

	"github.com/dukex/operion/pkg/models"
)

func (p *Persistence) DeleteFinishedExecutions(ctx context.Context, finishedBefore time.Time, limit int) (int, error) {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM execution_contexts
		WHERE id IN (
			SELECT id FROM execution_contexts
			WHERE status IN ($1, $2)
			  AND created_at < $3
			  AND completed_at < $3
			ORDER BY created_at
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		)
	`, models.ExecutionStatusCompleted, models.ExecutionStatusFailed, finishedBefore, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to delete finished executions: %w", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to delete finished executions: %w", err)
	}

	return int(deleted), nil
}
