package postgresql_test

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventDefinitionsSaveGetAndList(t *testing.T) {
	p, ctx, _ := setupTestDB(t)
	definitions := p.EventDefinitions()

	created := &models.EventDefinition{
		Key:        "employee_created",
		Name:       employeeCreatedName,
		Topic:      "tp_employee_created",
		Format:     models.EventFormatJSON,
		Schema:     map[string]any{"type": "object"},
		TenantFrom: models.TenantFromPayload,
		TenantKey:  "company_id",
	}
	require.NoError(t, definitions.Save(ctx, created))
	assert.False(t, created.CreatedAt.IsZero())

	updated := &models.EventDefinition{
		Key:        "employee_created",
		Name:       employeeCreatedName,
		Topic:      "tp_employee_created_v2",
		Format:     models.EventFormatAvro,
		TenantFrom: models.TenantFromHeader,
		TenantKey:  "x-company-id",
	}
	require.NoError(t, definitions.Save(ctx, updated))
	assert.True(t, created.CreatedAt.Equal(updated.CreatedAt))
	assert.False(t, updated.UpdatedAt.Before(updated.CreatedAt))

	loaded, err := definitions.Get(ctx, "employee_created")
	require.NoError(t, err)
	assert.Equal(t, "tp_employee_created_v2", loaded.Topic)
	assert.Equal(t, models.EventFormatAvro, loaded.Format)
	assert.Equal(t, models.TenantFromHeader, loaded.TenantFrom)
	assert.Nil(t, loaded.Schema)

	require.NoError(t, definitions.Save(ctx, &models.EventDefinition{
		Key:        "employee_dismissed",
		Name:       "Employee dismissed",
		Topic:      "tp_employee_dismissed",
		Format:     models.EventFormatJSON,
		TenantFrom: models.TenantFromPayload,
		TenantKey:  "company_id",
	}))

	all, err := definitions.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.Equal(t, "employee_created", all[0].Key)
	assert.Equal(t, "employee_dismissed", all[1].Key)

	_, err = definitions.Get(ctx, "missing")
	require.ErrorIs(t, err, persistence.ErrEventDefinitionNotFound)
}
