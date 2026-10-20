package postgresql

import (
	"context"
	"fmt"

	"github.com/dukex/operion/pkg/models"
)

func (p *Persistence) WebhookTriggers(ctx context.Context, tenantID, path string) ([]models.TriggerNodeMatch, error) {
	return p.publishedTriggers(ctx, models.NodeTypeTriggerWebhook, "webhook_path", path, &tenantID)
}

func (p *Persistence) KafkaTriggers(ctx context.Context, topic string) ([]models.TriggerNodeMatch, error) {
	return p.publishedTriggers(ctx, models.NodeTypeTriggerKafka, "topic", topic, nil)
}

func (p *Persistence) EventTriggers(ctx context.Context, eventKey, tenantID string) ([]models.TriggerNodeMatch, error) {
	return p.publishedTriggers(ctx, models.NodeTypeTriggerEvent, "event", eventKey, &tenantID)
}

func (p *Persistence) KafkaTopics(ctx context.Context) ([]string, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT n.config->>'topic'
		FROM workflow_nodes n
		JOIN workflows w ON w.id = n.workflow_id
		WHERE n.type = $1
		  AND COALESCE(n.config->>'topic', '') <> ''
		  AND w.status = $2
		  AND w.deleted_at IS NULL
		UNION
		SELECT d.topic
		FROM event_definitions d
		JOIN workflow_nodes n ON n.config->>'event' = d.key
		JOIN workflows w ON w.id = n.workflow_id
		WHERE n.type = $3
		  AND w.status = $2
		  AND w.deleted_at IS NULL
		ORDER BY 1
	`, models.NodeTypeTriggerKafka, models.WorkflowStatusPublished, models.NodeTypeTriggerEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to query kafka topics: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var topics []string

	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic); err != nil {
			return nil, fmt.Errorf("failed to scan kafka topic: %w", err)
		}

		topics = append(topics, topic)
	}

	return topics, rows.Err()
}

func (p *Persistence) publishedTriggers(ctx context.Context, nodeType, configKey, value string, tenantID *string) ([]models.TriggerNodeMatch, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT n.workflow_id, n.id
		FROM workflow_nodes n
		JOIN workflows w ON w.id = n.workflow_id
		WHERE n.type = $1
		  AND n.config->>$2 = $3
		  AND w.status = $4
		  AND w.deleted_at IS NULL
		  AND ($5::text IS NULL OR w.tenant_id = $5)
		ORDER BY n.workflow_id, n.id
	`, nodeType, configKey, value, models.WorkflowStatusPublished, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s triggers: %w", nodeType, err)
	}

	defer func() { _ = rows.Close() }()

	var matches []models.TriggerNodeMatch

	for rows.Next() {
		match := models.TriggerNodeMatch{TriggerNode: &models.WorkflowNode{}}
		if err := rows.Scan(&match.WorkflowID, &match.TriggerNode.ID); err != nil {
			return nil, fmt.Errorf("failed to scan %s trigger: %w", nodeType, err)
		}

		matches = append(matches, match)
	}

	return matches, rows.Err()
}
