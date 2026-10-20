package trigger_test

import (
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/trigger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	eventKey        = "event"
	employeeCreated = "employee_created"
)

func TestEventTriggerNeedsACatalogEvent(t *testing.T) {
	_, err := trigger.NewEventTriggerNode("on-employee", map[string]any{})
	require.ErrorContains(t, err, "event is required")

	_, err = trigger.NewEventTriggerNode("on-employee", map[string]any{eventKey: ""})
	require.ErrorContains(t, err, "event is required")

	node, err := trigger.NewEventTriggerNode("on-employee", map[string]any{eventKey: employeeCreated})
	require.NoError(t, err)
	assert.Equal(t, "on-employee", node.ID())
	assert.Equal(t, models.NodeTypeTriggerEvent, node.Type())
	assert.Equal(t, "on-employee:external", node.InputPorts()[0].ID)
	assert.Equal(t, "on-employee:success", node.OutputPorts()[0].ID)
}

func TestEventTriggerPassesTheEventOn(t *testing.T) {
	node, err := trigger.NewEventTriggerNode("on-employee", map[string]any{eventKey: employeeCreated})
	require.NoError(t, err)

	event := map[string]any{eventKey: employeeCreated, "tenant_id": "acme", "payload": map[string]any{"name": "Ana"}}

	results, err := node.Execute(models.ExecutionContext{}, map[string]models.NodeResult{"external": {Data: event}})
	require.NoError(t, err)
	assert.Equal(t, models.NodeResult{NodeID: "on-employee", Data: event, Status: string(models.NodeStatusSuccess)}, results["success"])

	_, err = node.Execute(models.ExecutionContext{}, map[string]models.NodeResult{})
	require.ErrorContains(t, err, "external input not found")
}

func TestEventTriggerFactory(t *testing.T) {
	factory := trigger.NewEventTriggerNodeFactory()
	assert.Equal(t, models.NodeTypeTriggerEvent, factory.ID())
	assert.NotEmpty(t, factory.Name())
	assert.NotEmpty(t, factory.Description())
	assert.Equal(t, []string{eventKey}, factory.Schema()["required"])

	node, err := factory.Create(t.Context(), "on-employee", map[string]any{eventKey: employeeCreated})
	require.NoError(t, err)
	assert.Equal(t, "on-employee", node.ID())
}
