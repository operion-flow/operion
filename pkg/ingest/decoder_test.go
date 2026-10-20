package ingest_test

import (
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/models"
	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	avroSchemaID     = 1
	protobufSchemaID = 2
	brokenSchemaID   = 3
	missingSchemaID  = 4
	garbageSchemaID  = 5
	failingSchemaID  = 500

	employeeName     = "Ana"
	employeeTenant   = "acme"
	tenantField      = "company_id"
	registryUsername = "operion"
	registryPassword = "secret"

	employeeAvro = `{
		"type": "record",
		"name": "Employee",
		"fields": [{"name": "name", "type": "string"}, {"name": "company_id", "type": "string"}]
	}`

	employeeProtobuf = `syntax = "proto3";

message EmployeeCreated {
  string name = 1;
  string company_id = 2;
}

message Employee {
  message Updated {
    string full_name = 1;
    string company_id = 2;
    bool active = 3;
  }
}
`
)

func schemaRegistry(t *testing.T) (ingest.SchemaRegistry, *atomic.Int32) {
	t.Helper()

	var calls atomic.Int32

	schemas := map[string]string{
		"/schemas/ids/1": employeeAvro,
		"/schemas/ids/2": employeeProtobuf,
		"/schemas/ids/3": "not a schema",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)

		username, password, _ := r.BasicAuth()
		schema, found := schemas[r.URL.Path]

		switch {
		case username != registryUsername || password != registryPassword:
			w.WriteHeader(http.StatusUnauthorized)
		case r.URL.Path == "/schemas/ids/500":
			w.WriteHeader(http.StatusInternalServerError)
		case r.URL.Path == "/schemas/ids/5":
			_, _ = w.Write([]byte("<html>"))
		case !found:
			http.NotFound(w, r)
		default:
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
		}
	}))
	t.Cleanup(server.Close)

	return ingest.SchemaRegistry{URL: server.URL + "/", Username: registryUsername, Password: registryPassword}, &calls
}

func confluent(id uint32, parts ...[]byte) []byte {
	value := binary.BigEndian.AppendUint32([]byte{0}, id)
	for _, part := range parts {
		value = append(value, part...)
	}

	return value
}

func messageIndexes(indexes ...int64) []byte {
	value := binary.AppendVarint(nil, int64(len(indexes)))
	for _, index := range indexes {
		value = binary.AppendVarint(value, index)
	}

	return value
}

func employee() map[string]any {
	var value map[string]any

	_ = json.Unmarshal(employeeCreated().Value, &value)

	return value
}

func avroEmployee(t *testing.T) []byte {
	t.Helper()

	value, err := avro.Marshal(avro.MustParse(employeeAvro), employee())
	require.NoError(t, err)

	return value
}

func protobufEmployee() []byte {
	value := protowire.AppendTag(nil, 1, protowire.BytesType)
	value = protowire.AppendString(value, employeeName)
	value = protowire.AppendTag(value, 2, protowire.BytesType)

	return protowire.AppendString(value, employeeTenant)
}

func registryKafka(registry ingest.SchemaRegistry, format models.EventFormat, starter *fakeStarter) *ingest.Kafka {
	definition := employeeCreatedDefinition(models.TenantFromPayload, tenantField)
	definition.Format = format
	catalog := fakeCatalog{definitions: []*models.EventDefinition{definition}}

	return ingest.NewKafka(subscribedTenants(), catalog, starter, registry, slog.New(slog.DiscardHandler))
}

func handleRegistryEvent(t *testing.T, registry ingest.SchemaRegistry, format models.EventFormat, value []byte) (*fakeStarter, error) {
	t.Helper()

	starter := &fakeStarter{}
	message := employeeCreated()
	message.Value = value

	return starter, registryKafka(registry, format, starter).Handle(t.Context(), message)
}

func TestEventDecodesAvroAndProtobufWithTheSchemaRegistry(t *testing.T) {
	tests := map[string]struct {
		format  models.EventFormat
		value   []byte
		payload map[string]any
	}{
		"avro": {
			format:  models.EventFormatAvro,
			value:   confluent(avroSchemaID, avroEmployee(t)),
			payload: employee(),
		},
		"protobuf first message": {
			format:  models.EventFormatProtobuf,
			value:   confluent(protobufSchemaID, []byte{0}, protobufEmployee()),
			payload: employee(),
		},
		"protobuf nested message": {
			format:  models.EventFormatProtobuf,
			value:   confluent(protobufSchemaID, messageIndexes(1, 0), protobufEmployee()),
			payload: map[string]any{"full_name": employeeName, tenantField: employeeTenant, "active": false},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			registry, _ := schemaRegistry(t)

			starter, err := handleRegistryEvent(t, registry, test.format, test.value)
			require.NoError(t, err)

			require.Len(t, starter.started, 1)
			assert.Equal(t, "acme-onboarding", starter.started[0].execution.WorkflowID)
			assert.Equal(t, employeeTenant, starter.started[0].job.Data["tenant_id"])
			assert.Equal(t, test.payload, starter.started[0].job.Data["payload"])
		})
	}
}

func TestEventFetchesEachSchemaOnce(t *testing.T) {
	registry, calls := schemaRegistry(t)
	starter := &fakeStarter{}
	kafka := registryKafka(registry, models.EventFormatAvro, starter)

	first := employeeCreated()
	first.Value = confluent(avroSchemaID, avroEmployee(t))
	second := first
	second.Offset++

	require.NoError(t, kafka.Handle(t.Context(), first))
	require.NoError(t, kafka.Handle(t.Context(), second))

	assert.Len(t, starter.started, 2)
	assert.Equal(t, int32(1), calls.Load())
}

func TestEventsThatCannotBeDecodedAreSkipped(t *testing.T) {
	tests := map[string]struct {
		format models.EventFormat
		value  []byte
	}{
		"wrong magic byte":            {format: models.EventFormatAvro, value: append([]byte{1}, confluent(avroSchemaID, avroEmployee(t))[1:]...)},
		"too short":                   {format: models.EventFormatAvro, value: []byte{0, 0, 0}},
		"schema not found":            {format: models.EventFormatAvro, value: confluent(missingSchemaID, avroEmployee(t))},
		"broken avro schema":          {format: models.EventFormatAvro, value: confluent(brokenSchemaID, avroEmployee(t))},
		"avro payload":                {format: models.EventFormatAvro, value: confluent(avroSchemaID, []byte{6, 'A'})},
		"broken protobuf schema":      {format: models.EventFormatProtobuf, value: confluent(brokenSchemaID, []byte{0}, protobufEmployee())},
		"no message indexes":          {format: models.EventFormatProtobuf, value: confluent(protobufSchemaID)},
		"truncated message indexes":   {format: models.EventFormatProtobuf, value: confluent(protobufSchemaID, binary.AppendVarint(nil, 1))},
		"message index out of schema": {format: models.EventFormatProtobuf, value: confluent(protobufSchemaID, messageIndexes(5), protobufEmployee())},
		"protobuf payload":            {format: models.EventFormatProtobuf, value: confluent(protobufSchemaID, []byte{0, 0xff})},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			registry, _ := schemaRegistry(t)

			starter, err := handleRegistryEvent(t, registry, test.format, test.value)
			require.NoError(t, err)

			assert.Empty(t, starter.started)
		})
	}
}

func TestEventReturnsSchemaRegistryFailures(t *testing.T) {
	registry, _ := schemaRegistry(t)

	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()

	wrongPassword := registry
	wrongPassword.Password = "wrong"

	tests := map[string]struct {
		registry ingest.SchemaRegistry
		id       uint32
	}{
		"server error":    {registry: registry, id: failingSchemaID},
		"not JSON":        {registry: registry, id: garbageSchemaID},
		"wrong password":  {registry: wrongPassword, id: avroSchemaID},
		"unreachable":     {registry: ingest.SchemaRegistry{URL: closed.URL}, id: avroSchemaID},
		"invalid address": {registry: ingest.SchemaRegistry{URL: "http://registry\x7f"}, id: avroSchemaID},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			starter, err := handleRegistryEvent(t, test.registry, models.EventFormatAvro, confluent(test.id, avroEmployee(t)))
			require.ErrorContains(t, err, "schema registry is unavailable")

			assert.Empty(t, starter.started)
		})
	}
}
