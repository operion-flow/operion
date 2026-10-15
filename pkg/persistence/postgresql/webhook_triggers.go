package postgresql

import (
	"context"
	"fmt"

	"github.com/dukex/operion/pkg/models"
)

func (p *Persistence) WebhookTriggers(ctx context.Context, path string) ([]models.TriggerNodeMatch, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT n.workflow_id, n.id
		FROM workflow_nodes n
		JOIN workflows w ON w.id = n.workflow_id
		WHERE n.type = 'trigger:webhook'
		  AND n.config->>'webhook_path' = $1
		  AND w.status = $2
		  AND w.deleted_at IS NULL
		ORDER BY n.workflow_id, n.id
	`, path, models.WorkflowStatusPublished)
	if err != nil {
		return nil, fmt.Errorf("failed to query webhook triggers: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var matches []models.TriggerNodeMatch

	for rows.Next() {
		var match models.TriggerNodeMatch

		match.TriggerNode = &models.WorkflowNode{}
		if err := rows.Scan(&match.WorkflowID, &match.TriggerNode.ID); err != nil {
			return nil, fmt.Errorf("failed to scan webhook trigger: %w", err)
		}

		matches = append(matches, match)
	}

	return matches, rows.Err()
}
