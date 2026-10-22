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

func saveVersion(ctx context.Context, t *testing.T, p *postgresql.Persistence, group, tenant string, status models.WorkflowStatus) string {
	t.Helper()

	id := uuid.NewString()
	if group == "" {
		group = id
	}

	require.NoError(t, p.WorkflowRepository().Save(ctx, &models.Workflow{
		ID:              id,
		Name:            "versioned workflow",
		Description:     "one version of a group",
		Status:          status,
		WorkflowGroupID: group,
		TenantID:        tenant,
	}))

	return id
}

func statusOf(ctx context.Context, t *testing.T, p *postgresql.Persistence, id string) models.WorkflowStatus {
	t.Helper()

	wf, err := p.WorkflowRepository().GetByID(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, wf)

	return wf.Status
}

func TestPublishingANewVersionUnpublishesTheOldOne(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	first := saveVersion(ctx, t, p, "", acmeTenant, models.WorkflowStatusPublished)
	second := saveVersion(ctx, t, p, first, acmeTenant, models.WorkflowStatusDraft)

	require.NoError(t, p.WorkflowRepository().PublishWorkflow(ctx, second))

	assert.Equal(t, models.WorkflowStatusUnpublished, statusOf(ctx, t, p, first))
	assert.Equal(t, models.WorkflowStatusPublished, statusOf(ctx, t, p, second))
}

func TestPublishingNeverUnpublishesAnotherTenantsWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	acme := saveVersion(ctx, t, p, "", acmeTenant, models.WorkflowStatusPublished)
	globex := saveVersion(ctx, t, p, acme, "globex", models.WorkflowStatusDraft)

	require.NoError(t, p.WorkflowRepository().PublishWorkflow(ctx, globex))

	assert.Equal(t, models.WorkflowStatusPublished, statusOf(ctx, t, p, acme))
	assert.Equal(t, models.WorkflowStatusPublished, statusOf(ctx, t, p, globex))
}
