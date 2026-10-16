package ingest_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/dukex/operion/pkg/ingest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sessionScript func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error

type fakeGroup struct {
	sarama.ConsumerGroup

	mu       sync.Mutex
	sessions []sessionScript
	topics   [][]string
	done     func()
}

func (g *fakeGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	g.mu.Lock()
	g.topics = append(g.topics, topics)

	if len(g.sessions) == 0 {
		g.mu.Unlock()
		g.done()

		return nil
	}

	next := g.sessions[0]
	g.sessions = g.sessions[1:]
	g.mu.Unlock()

	return next(ctx, topics, handler)
}

type fakeSession struct {
	sarama.ConsumerGroupSession

	ctx    context.Context //nolint:containedctx // a session carries its own context in sarama
	marked []int64
}

func (s *fakeSession) Context() context.Context {
	return s.ctx
}

func (s *fakeSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, message.Offset)
}

type fakeClaim struct {
	sarama.ConsumerGroupClaim

	messages chan *sarama.ConsumerMessage
}

func (c fakeClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

func claimOf(closed bool, messages ...*sarama.ConsumerMessage) fakeClaim {
	channel := make(chan *sarama.ConsumerMessage, len(messages))
	for _, message := range messages {
		channel <- message
	}

	if closed {
		close(channel)
	}

	return fakeClaim{messages: channel}
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

func runConsumer(t *testing.T, group *fakeGroup, topics ingest.KafkaTopics, handle func(context.Context, ingest.KafkaMessage) error) error {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	group.done = cancel

	return ingest.NewKafkaConsumer(group, topics, handle, time.Millisecond, slog.New(slog.DiscardHandler)).Run(ctx)
}

func TestConsumerHandlesAndMarksMessages(t *testing.T) {
	var handled []ingest.KafkaMessage

	session := &fakeSession{}
	group := &fakeGroup{sessions: []sessionScript{
		func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
			session.ctx = ctx

			if err := handler.Setup(session); err != nil {
				return err
			}

			defer func() { _ = handler.Cleanup(session) }()

			return handler.ConsumeClaim(session, claimOf(true,
				&sarama.ConsumerMessage{Topic: employeesTopic, Offset: 7, Key: []byte("emp-1"), Value: []byte(`{}`), Headers: []*sarama.RecordHeader{{Key: []byte(sourceHeader), Value: []byte("hr")}}},
				&sarama.ConsumerMessage{Topic: employeesTopic, Offset: 8},
			))
		},
	}}

	err := runConsumer(t, group, always(employeesTopic), func(_ context.Context, message ingest.KafkaMessage) error {
		handled = append(handled, message)

		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, [][]string{{employeesTopic}, {employeesTopic}}, group.topics)
	require.Len(t, handled, 2)
	assert.Equal(t, int64(7), handled[0].Offset)
	assert.Equal(t, []byte("emp-1"), handled[0].Key)
	assert.Equal(t, map[string]string{sourceHeader: "hr"}, handled[0].Headers)
	assert.Equal(t, []int64{7, 8}, session.marked)
}

func TestConsumerEndsTheSessionWithoutMarkingAFailedMessage(t *testing.T) {
	errStart := errors.New("database is down")
	session := &fakeSession{}

	var sessionErr error

	group := &fakeGroup{sessions: []sessionScript{
		func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
			session.ctx = ctx
			sessionErr = handler.ConsumeClaim(session, claimOf(false, &sarama.ConsumerMessage{Topic: employeesTopic, Offset: 9}))

			<-ctx.Done()

			return nil
		},
	}}

	err := runConsumer(t, group, always(employeesTopic), func(context.Context, ingest.KafkaMessage) error {
		return errStart
	})

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, sessionErr, errStart)
	assert.Empty(t, session.marked)
	assert.Len(t, group.topics, 2)
}

func TestConsumerRestartsWhenTopicsChange(t *testing.T) {
	topics := &fakeTopics{answers: func(call int) ([]string, error) {
		if call == 1 {
			return []string{"a"}, nil
		}

		return []string{"a", "b"}, nil
	}}
	session := &fakeSession{}
	group := &fakeGroup{sessions: []sessionScript{
		func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
			session.ctx = ctx

			return handler.ConsumeClaim(session, claimOf(false))
		},
	}}

	err := runConsumer(t, group, topics, func(context.Context, ingest.KafkaMessage) error { return nil })

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, [][]string{{"a"}, {"a", "b"}}, group.topics)
}

func TestConsumerWaitsWhenThereIsNothingToConsume(t *testing.T) {
	group := &fakeGroup{}
	topics := &fakeTopics{answers: func(call int) ([]string, error) {
		switch call {
		case 1:
			return nil, errors.New("database is down")
		case 2:
			return nil, nil
		default:
			group.done()

			return nil, nil
		}
	}}

	ctx, cancel := context.WithCancel(t.Context())
	group.done = cancel

	err := ingest.NewKafkaConsumer(group, topics, nil, time.Millisecond, slog.New(slog.DiscardHandler)).Run(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, topics.calls, 3)
	assert.Empty(t, group.topics)
}

func TestConsumerRetriesWhenTheGroupFails(t *testing.T) {
	group := &fakeGroup{sessions: []sessionScript{
		func(context.Context, []string, sarama.ConsumerGroupHandler) error {
			return errors.New("broker unavailable")
		},
	}}

	err := runConsumer(t, group, always(employeesTopic), nil)

	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, group.topics, 2)
}
