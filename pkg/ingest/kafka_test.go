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
)

type fakeKafkaTriggers struct {
	matches map[string][]models.TriggerNodeMatch
	err     error
}

func (f fakeKafkaTriggers) KafkaTriggers(_ context.Context, topic string) ([]models.TriggerNodeMatch, error) {
	return f.matches[topic], f.err
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

	return ingest.NewKafka(triggers, starter, slog.New(slog.DiscardHandler)).Handle(t.Context(), message)
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
		"partition":    int32(2),
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
