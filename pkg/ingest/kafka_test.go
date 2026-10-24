package ingest_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	employeesTopic = "employees"
	sourceHeader   = "source"
	acmeEmployee   = `{"company_id": "acme"}`
)

type fakeKafkaTriggers struct {
	matches   map[string][]models.TriggerNodeMatch
	events    map[string][]models.TriggerNodeMatch
	err       error
	eventsErr error
}

func (f fakeKafkaTriggers) KafkaTriggers(_ context.Context, topic string) ([]models.TriggerNodeMatch, error) {
	return f.matches[topic], f.err
}

func (f fakeKafkaTriggers) EventTriggers(_ context.Context, eventKey, tenantID string) ([]models.TriggerNodeMatch, error) {
	return f.events[eventKey+"/"+tenantID], f.eventsErr
}

type fakeCatalog struct {
	definitions []*models.EventDefinition
	err         error
}

func (f fakeCatalog) ByTopic(_ context.Context, topic string) ([]*models.EventDefinition, error) {
	var found []*models.EventDefinition

	for _, definition := range f.definitions {
		if definition.Topic == topic {
			found = append(found, definition)
		}
	}

	return found, f.err
}

func employeeCreated() ingest.KafkaMessage {
	return ingest.KafkaMessage{
		Topic:     employeesTopic,
		Partition: 2,
		Offset:    41,
		Key:       []byte("emp-1"),
		Value:     []byte(`{"name": "Ana", "company_id": "acme"}`),
		Headers:   map[string]string{sourceHeader: "hr"},
		Timestamp: time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
	}
}

func handleKafka(t *testing.T, triggers fakeKafkaTriggers, starter *fakeStarter, message ingest.KafkaMessage) error {
	t.Helper()

	return handleEvents(t, triggers, fakeCatalog{}, starter, message)
}

func handleEvents(t *testing.T, triggers fakeKafkaTriggers, catalog fakeCatalog, starter *fakeStarter, message ingest.KafkaMessage) error {
	t.Helper()

	return ingest.NewKafka(triggers, catalog, starter, ingest.SchemaRegistry{}, slog.New(slog.DiscardHandler)).Handle(t.Context(), message)
}

func listening() fakeKafkaTriggers {
	return fakeKafkaTriggers{matches: map[string][]models.TriggerNodeMatch{
		employeesTopic: {match("wf-1", "trigger"), match("wf-2", "on-employee")},
	}}
}

func TestKafkaStartsOneExecutionPerWorkflowOnTheTopic(t *testing.T) {
	starter := &fakeStarter{}

	require.NoError(t, handleKafka(t, listening(), starter, employeeCreated()))

	require.Len(t, starter.started, 2)

	first := starter.started[0]
	assert.Equal(t, "wf-1", first.execution.WorkflowID)
	assert.Equal(t, models.ExecutionStatusRunning, first.execution.Status)
	assert.Equal(t, first.execution.ID, first.job.ExecutionID)
	assert.Equal(t, "trigger", first.job.NodeID)
	assert.Equal(t, "external", first.job.Port)
	assert.Equal(t, map[string]any{
		"topic":        employeesTopic,
		"partition":    2,
		"offset":       int64(41),
		"message_key":  "emp-1",
		"message_data": map[string]any{"name": "Ana", "company_id": "acme"},
		"headers":      map[string]any{sourceHeader: "hr"},
		"timestamp":    "2026-10-15T12:00:00Z",
	}, first.job.Data)
	assert.Equal(t, first.job.Data, first.execution.TriggerData)
	assert.Equal(t, "on-employee", starter.started[1].job.NodeID)
}

func TestKafkaIDsAreStableForTheSameMessage(t *testing.T) {
	first := &fakeStarter{}
	again := &fakeStarter{}

	require.NoError(t, handleKafka(t, listening(), first, employeeCreated()))
	require.NoError(t, handleKafka(t, listening(), again, employeeCreated()))

	for i := range first.started {
		assert.Equal(t, first.started[i].execution.ID, again.started[i].execution.ID)
		assert.Equal(t, first.started[i].job.ID, again.started[i].job.ID)
		assert.NotEqual(t, first.started[i].execution.ID, first.started[i].job.ID)
	}

	assert.NotEqual(t, first.started[0].execution.ID, first.started[1].execution.ID)

	next := employeeCreated()
	next.Offset++
	other := &fakeStarter{}
	require.NoError(t, handleKafka(t, listening(), other, next))
	assert.NotEqual(t, first.started[0].execution.ID, other.started[0].execution.ID)
}

func TestKafkaKeepsMessagesThatAreNotJSON(t *testing.T) {
	starter := &fakeStarter{}
	message := employeeCreated()
	message.Value = []byte("plain text")

	require.NoError(t, handleKafka(t, listening(), starter, message))

	assert.Equal(t, map[string]any{"raw_message": "plain text"}, starter.started[0].job.Data["message_data"])
}

func TestKafkaIgnoresTopicsWithoutWorkflows(t *testing.T) {
	starter := &fakeStarter{}

	require.NoError(t, handleKafka(t, fakeKafkaTriggers{}, starter, employeeCreated()))

	assert.Empty(t, starter.started)
}

func TestKafkaReturnsLookupAndStartErrors(t *testing.T) {
	errLookup := errors.New("lookup failed")
	errStart := errors.New("start failed")

	require.ErrorIs(t, handleKafka(t, fakeKafkaTriggers{err: errLookup}, &fakeStarter{}, employeeCreated()), errLookup)
	require.ErrorIs(t, handleKafka(t, listening(), &fakeStarter{err: errStart}, employeeCreated()), errStart)
}

func employeeCreatedDefinition(from models.TenantSource, key string) *models.EventDefinition {
	return &models.EventDefinition{
		Key:        "employee_created",
		Name:       "Employee created",
		Topic:      employeesTopic,
		Format:     models.EventFormatJSON,
		TenantFrom: from,
		TenantKey:  key,
	}
}

func subscribedTenants() fakeKafkaTriggers {
	return fakeKafkaTriggers{events: map[string][]models.TriggerNodeMatch{
		"employee_created/acme":   {match("acme-onboarding", "on-employee")},
		"employee_created/globex": {match("globex-onboarding", "on-employee")},
		"employee_created/42":     {match("numeric-onboarding", "on-employee")},
	}}
}

func TestEventStartsOnlyTheWorkflowsOfThePayloadTenant(t *testing.T) {
	starter := &fakeStarter{}
	catalog := fakeCatalog{definitions: []*models.EventDefinition{employeeCreatedDefinition(models.TenantFromPayload, "company_id")}}

	require.NoError(t, handleEvents(t, subscribedTenants(), catalog, starter, employeeCreated()))

	require.Len(t, starter.started, 1)

	started := starter.started[0]
	assert.Equal(t, "acme-onboarding", started.execution.WorkflowID)
	assert.Equal(t, "on-employee", started.job.NodeID)
	assert.Equal(t, "external", started.job.Port)
	assert.Equal(t, map[string]any{
		"event":     "employee_created",
		"tenant_id": "acme",
		"payload":   map[string]any{"name": "Ana", "company_id": "acme"},
		"headers":   map[string]any{sourceHeader: "hr"},
		"key":       "emp-1",
		"topic":     employeesTopic,
		"partition": 2,
		"offset":    int64(41),
		"timestamp": "2026-10-15T12:00:00Z",
	}, started.job.Data)
	assert.Equal(t, started.job.Data, started.execution.TriggerData)
}

func TestEventReadsTheTenantFromANestedFieldOrAHeader(t *testing.T) {
	tests := map[string]struct {
		definition *models.EventDefinition
		value      string
		headers    map[string]string
		workflow   string
	}{
		"nested field": {
			definition: employeeCreatedDefinition(models.TenantFromPayload, "company.id"),
			value:      `{"name": "Ana", "company": {"id": "globex"}}`,
			workflow:   "globex-onboarding",
		},
		"numeric field": {
			definition: employeeCreatedDefinition(models.TenantFromPayload, "company.id"),
			value:      `{"name": "Ana", "company": {"id": 42}}`,
			workflow:   "numeric-onboarding",
		},
		"header": {
			definition: employeeCreatedDefinition(models.TenantFromHeader, "x-company-id"),
			value:      `{"name": "Ana"}`,
			headers:    map[string]string{"x-company-id": "globex"},
			workflow:   "globex-onboarding",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			starter := &fakeStarter{}
			message := employeeCreated()
			message.Value = []byte(test.value)
			message.Headers = test.headers

			require.NoError(t, handleEvents(t, subscribedTenants(), fakeCatalog{definitions: []*models.EventDefinition{test.definition}}, starter, message))

			require.Len(t, starter.started, 1)
			assert.Equal(t, test.workflow, starter.started[0].execution.WorkflowID)
		})
	}
}

func TestEventsThatCannotBeReadAreSkipped(t *testing.T) {
	avro := employeeCreatedDefinition(models.TenantFromPayload, "company_id")
	avro.Format = models.EventFormatAvro

	tests := map[string]struct {
		definition *models.EventDefinition
		value      string
	}{
		"not JSON":           {definition: employeeCreatedDefinition(models.TenantFromPayload, "company_id"), value: "plain text"},
		"not an object":      {definition: employeeCreatedDefinition(models.TenantFromPayload, "company_id"), value: `["acme"]`},
		"no tenant":          {definition: employeeCreatedDefinition(models.TenantFromPayload, "company_id"), value: `{"name": "Ana"}`},
		"tenant not a value": {definition: employeeCreatedDefinition(models.TenantFromPayload, "company_id.id"), value: acmeEmployee},
		"no tenant header":   {definition: employeeCreatedDefinition(models.TenantFromHeader, "x-company-id"), value: acmeEmployee},
		"no schema registry": {definition: avro, value: acmeEmployee},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			starter := &fakeStarter{}
			message := employeeCreated()
			message.Value = []byte(test.value)

			require.NoError(t, handleEvents(t, subscribedTenants(), fakeCatalog{definitions: []*models.EventDefinition{test.definition}}, starter, message))

			assert.Empty(t, starter.started)
		})
	}
}

func TestRawAndEventTriggersOnTheSameTopicGetTheirOwnExecutions(t *testing.T) {
	starter := &fakeStarter{}
	triggers := subscribedTenants()
	triggers.matches = map[string][]models.TriggerNodeMatch{employeesTopic: {match("acme-onboarding", "raw")}}
	catalog := fakeCatalog{definitions: []*models.EventDefinition{employeeCreatedDefinition(models.TenantFromPayload, "company_id")}}

	require.NoError(t, handleEvents(t, triggers, catalog, starter, employeeCreated()))

	require.Len(t, starter.started, 2)
	assert.NotEqual(t, starter.started[0].execution.ID, starter.started[1].execution.ID)
	assert.NotEqual(t, starter.started[0].job.ID, starter.started[1].job.ID)
}

func TestEventReturnsCatalogAndLookupErrors(t *testing.T) {
	errCatalog := errors.New("catalog failed")
	errLookup := errors.New("lookup failed")
	catalog := fakeCatalog{definitions: []*models.EventDefinition{employeeCreatedDefinition(models.TenantFromPayload, "company_id")}}
	failing := subscribedTenants()
	failing.eventsErr = errLookup

	require.ErrorIs(t, handleEvents(t, subscribedTenants(), fakeCatalog{err: errCatalog}, &fakeStarter{}, employeeCreated()), errCatalog)
	require.ErrorIs(t, handleEvents(t, failing, catalog, &fakeStarter{}, employeeCreated()), errLookup)
}
