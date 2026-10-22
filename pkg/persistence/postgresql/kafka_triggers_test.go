package postgresql_test

import (
	"context"
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func saveKafkaWorkflow(ctx context.Context, t *testing.T, p *postgresql.Persistence, topic string, status models.WorkflowStatus) string {
	t.Helper()

	id := uuid.NewString()
	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              id,
		Name:            "employee created",
		Description:     "reacts to new employees",
		Status:          status,
		WorkflowGroupID: id,
		Nodes: []*models.WorkflowNode{
			{ID: triggerNodeID, Name: triggerNodeID, Type: models.NodeTypeTriggerKafka, Category: models.CategoryTypeTrigger, Config: map[string]any{"topic": topic}},
		},
	}))

	return id
}

func TestKafkaTriggersAndTopicsComeFromPublishedWorkflows(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	first := saveKafkaWorkflow(ctx, t, p, "employees", models.WorkflowStatusPublished)
	second := saveKafkaWorkflow(ctx, t, p, "employees", models.WorkflowStatusPublished)
	saveKafkaWorkflow(ctx, t, p, "payments", models.WorkflowStatusPublished)
	saveKafkaWorkflow(ctx, t, p, "drafts", models.WorkflowStatusDraft)
	saveWebhookWorkflow(ctx, t, p, "/employees", models.WorkflowStatusPublished)

	topics, err := p.KafkaTopics(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"employees", "payments"}, topics)

	matches, err := p.KafkaTriggers(ctx, "employees")
	require.NoError(t, err)
	require.Len(t, matches, 2)

	workflows := []string{matches[0].WorkflowID, matches[1].WorkflowID}
	assert.ElementsMatch(t, []string{first, second}, workflows)
	assert.Equal(t, triggerNodeID, matches[0].TriggerNode.ID)

	matches, err = p.KafkaTriggers(ctx, "drafts")
	require.NoError(t, err)
	assert.Empty(t, matches)
}
