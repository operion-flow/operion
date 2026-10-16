package ingest_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/ingest"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeReader struct {
	topics    []string
	messages  []kafka.Message
	fetchErr  error
	commitErr error
	committed []int64
	closed    bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.fetchErr != nil {
		return kafka.Message{}, r.fetchErr
	}

	if len(r.messages) == 0 {
		<-ctx.Done()

		return kafka.Message{}, ctx.Err()
	}

	message := r.messages[0]
	r.messages = r.messages[1:]

	return message, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	if r.commitErr != nil {
		return r.commitErr
	}

	for _, message := range messages {
		r.committed = append(r.committed, message.Offset)
	}

	return nil
}

func (r *fakeReader) Close() error {
	r.closed = true

	return nil
}

type readers struct {
	mu      sync.Mutex
	scripts []*fakeReader
	opened  []*fakeReader
	done    func()
}

func (r *readers) open(topics []string) ingest.KafkaReader {
	r.mu.Lock()
	defer r.mu.Unlock()

	next := &fakeReader{}

	if len(r.scripts) == 0 {
		r.done()
	} else {
		next = r.scripts[0]
		r.scripts = r.scripts[1:]
	}

	next.topics = topics
	r.opened = append(r.opened, next)

	return next
}

type fakeTopics struct {
	mu      sync.Mutex
	calls   int
	answers func(call int) ([]string, error)
}

func (f *fakeTopics) KafkaTopics(context.Context) ([]string, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.mu.Unlock()

	return f.answers(call)
}

func always(topics ...string) *fakeTopics {
	return &fakeTopics{answers: func(int) ([]string, error) { return topics, nil }}
}

func runConsumer(t *testing.T, r *readers, topics ingest.KafkaTopics, handle func(context.Context, ingest.KafkaMessage) error) error {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	r.done = cancel

	return ingest.NewKafkaConsumer(r.open, topics, handle, time.Millisecond, slog.New(slog.DiscardHandler)).Run(ctx)
}

func TestConsumerHandlesAndCommitsMessages(t *testing.T) {
	var handled []ingest.KafkaMessage

	reader := &fakeReader{messages: []kafka.Message{
		{Topic: employeesTopic, Partition: 3, Offset: 7, Key: []byte("emp-1"), Value: []byte(`{}`), Headers: []kafka.Header{{Key: sourceHeader, Value: []byte("hr")}}},
		{Topic: employeesTopic, Offset: 8},
	}}
	r := &readers{scripts: []*fakeReader{reader}}

	err := runConsumer(t, r, always(employeesTopic), func(_ context.Context, message ingest.KafkaMessage) error {
		handled = append(handled, message)

		if message.Offset == 8 {
			r.done()
		}

		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, handled, 2)
	assert.Equal(t, 3, handled[0].Partition)
	assert.Equal(t, []byte("emp-1"), handled[0].Key)
	assert.Equal(t, map[string]string{sourceHeader: "hr"}, handled[0].Headers)
	assert.Equal(t, []int64{7, 8}, reader.committed)
	assert.Equal(t, []string{employeesTopic}, reader.topics)
	assert.True(t, reader.closed)
}

func TestConsumerReopensWithoutCommittingAFailedMessage(t *testing.T) {
	errStart := errors.New("database is down")
	failing := &fakeReader{messages: []kafka.Message{{Topic: employeesTopic, Offset: 9}}}
	r := &readers{scripts: []*fakeReader{failing}}

	err := runConsumer(t, r, always(employeesTopic), func(context.Context, ingest.KafkaMessage) error {
		return errStart
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, failing.committed)
	assert.True(t, failing.closed)
	assert.Len(t, r.opened, 2)
}

func TestConsumerReopensWhenReadingOrCommittingFails(t *testing.T) {
	r := &readers{scripts: []*fakeReader{
		{fetchErr: errors.New("broker unavailable")},
		{messages: []kafka.Message{{Topic: employeesTopic, Offset: 1}}, commitErr: errors.New("commit failed")},
	}}

	err := runConsumer(t, r, always(employeesTopic), func(context.Context, ingest.KafkaMessage) error { return nil })

	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, r.opened, 3)
}

func TestConsumerReopensWhenTopicsChange(t *testing.T) {
	topics := &fakeTopics{answers: func(call int) ([]string, error) {
		if call == 1 {
			return []string{"a"}, nil
		}

		return []string{"a", "b"}, nil
	}}
	r := &readers{scripts: []*fakeReader{{}}}

	err := runConsumer(t, r, topics, func(context.Context, ingest.KafkaMessage) error { return nil })

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, r.opened, 2)
	assert.Equal(t, []string{"a"}, r.opened[0].topics)
	assert.Equal(t, []string{"a", "b"}, r.opened[1].topics)
}

func TestConsumerWaitsWhenThereIsNothingToConsume(t *testing.T) {
	r := &readers{}
	topics := &fakeTopics{answers: func(call int) ([]string, error) {
		switch call {
		case 1:
			return nil, errors.New("database is down")
		case 2:
			return nil, nil
		default:
			r.done()

			return nil, nil
		}
	}}

	ctx, cancel := context.WithCancel(t.Context())
	r.done = cancel

	err := ingest.NewKafkaConsumer(r.open, topics, nil, time.Millisecond, slog.New(slog.DiscardHandler)).Run(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, topics.calls, 3)
	assert.Empty(t, r.opened)
}
