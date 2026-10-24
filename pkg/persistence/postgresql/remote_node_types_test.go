package postgresql_test

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	approvedPort = "approved"
	errorPort    = "error"
)

func TestRemoteNodeTypesSaveAndGet(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	types := p.RemoteNodeTypes()

	created := &models.RemoteNodeType{
		ID:             "create_benefit",
		Name:           "Create benefit",
		URL:            "http://benefits.internal/run",
		Outputs:        []string{approvedPort, errorPort},
		Schema:         map[string]any{"required": []any{"amount"}},
		TimeoutSeconds: 10,
	}
	require.NoError(t, types.Save(ctx, created))
	assert.False(t, created.CreatedAt.IsZero())

	updated := &models.RemoteNodeType{
		ID:             "create_benefit",
		Name:           "Create benefit",
		Description:    "Creates the benefit in the benefits system",
		URL:            "http://benefits.internal/v2/run",
		Secret:         "a-secret-of-sixteen-bytes",
		Outputs:        []string{approvedPort, "rejected", errorPort},
		TimeoutSeconds: 20,
	}
	require.NoError(t, types.Save(ctx, updated))
	assert.True(t, created.CreatedAt.Equal(updated.CreatedAt))
	assert.False(t, updated.UpdatedAt.Before(updated.CreatedAt))

	loaded, err := types.Get(ctx, "create_benefit")
	require.NoError(t, err)
	assert.Equal(t, "http://benefits.internal/v2/run", loaded.URL)
	assert.Equal(t, "a-secret-of-sixteen-bytes", loaded.Secret)
	assert.Equal(t, "Creates the benefit in the benefits system", loaded.Description)
	assert.Equal(t, []string{approvedPort, "rejected", errorPort}, loaded.Outputs)
	assert.Equal(t, 20, loaded.TimeoutSeconds)
	assert.Nil(t, loaded.Schema)

	require.Error(t, types.Save(ctx, &models.RemoteNodeType{ID: "broken", Schema: map[string]any{"bad": make(chan int)}}))

	_, err = types.Get(ctx, "missing")
	require.ErrorIs(t, err, persistence.ErrRemoteNodeTypeNotFound)
}
