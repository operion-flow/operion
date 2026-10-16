package ingest

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type KafkaTopics interface {
	KafkaTopics(ctx context.Context) ([]string, error)
}

type KafkaConsumer struct {
	group   sarama.ConsumerGroup
	topics  KafkaTopics
	handle  func(context.Context, KafkaMessage) error
	refresh time.Duration
	logger  *slog.Logger
}

func NewKafkaConsumer(
	group sarama.ConsumerGroup,
	topics KafkaTopics,
	handle func(context.Context, KafkaMessage) error,
	refresh time.Duration,
	logger *slog.Logger,
) *KafkaConsumer {
	return &KafkaConsumer{group: group, topics: topics, handle: handle, refresh: refresh, logger: logger}
}

func (c *KafkaConsumer) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		topics, err := c.topics.KafkaTopics(ctx)

		switch {
		case err != nil:
			c.logger.ErrorContext(ctx, "failed to list kafka topics", "error", err)
		case len(topics) == 0:
		default:
			err = c.consume(ctx, topics)
			if err == nil {
				continue
			}

			c.logger.ErrorContext(ctx, "kafka session ended", "topics", topics, "error", err)
		}

		select {
		case <-ctx.Done():
		case <-time.After(c.refresh):
		}
	}
}

func (c *KafkaConsumer) consume(ctx context.Context, topics []string) error {
	session, cancel := context.WithCancel(ctx)
	defer cancel()

	go c.cancelOnTopicChange(session, cancel, topics)

	handler := &claimHandler{handle: c.handle, cancel: cancel}
	if err := c.group.Consume(session, topics, handler); err != nil {
		return err
	}

	return handler.failure()
}

func (c *KafkaConsumer) cancelOnTopicChange(ctx context.Context, cancel context.CancelFunc, current []string) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.refresh):
		}

		topics, err := c.topics.KafkaTopics(ctx)
		if err == nil && !slices.Equal(topics, current) {
			c.logger.InfoContext(ctx, "kafka topics changed", "from", current, "to", topics)
			cancel()

			return
		}
	}
}

type claimHandler struct {
	handle func(context.Context, KafkaMessage) error
	cancel context.CancelFunc

	mu  sync.Mutex
	err error
}

func (*claimHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (*claimHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *claimHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			if err := h.handle(session.Context(), toKafkaMessage(message)); err != nil {
				h.fail(err)

				return err
			}

			session.MarkMessage(message, "")
		}
	}
}

func (h *claimHandler) fail(err error) {
	h.mu.Lock()
	if h.err == nil {
		h.err = err
	}
	h.mu.Unlock()

	h.cancel()
}

func (h *claimHandler) failure() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.err
}

func toKafkaMessage(message *sarama.ConsumerMessage) KafkaMessage {
	headers := make(map[string]string, len(message.Headers))
	for _, header := range message.Headers {
		headers[string(header.Key)] = string(header.Value)
	}

	return KafkaMessage{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       message.Key,
		Value:     message.Value,
		Headers:   headers,
		Timestamp: message.Timestamp,
	}
}
