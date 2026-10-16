package ingest

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaTopics interface {
	KafkaTopics(ctx context.Context) ([]string, error)
}

type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

type KafkaConsumer struct {
	newReader func(topics []string) KafkaReader
	topics    KafkaTopics
	handle    func(context.Context, KafkaMessage) error
	refresh   time.Duration
	logger    *slog.Logger
}

func NewKafkaConsumer(
	newReader func(topics []string) KafkaReader,
	topics KafkaTopics,
	handle func(context.Context, KafkaMessage) error,
	refresh time.Duration,
	logger *slog.Logger,
) *KafkaConsumer {
	return &KafkaConsumer{newReader: newReader, topics: topics, handle: handle, refresh: refresh, logger: logger}
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

			c.logger.ErrorContext(ctx, "kafka reader stopped", "topics", topics, "error", err)
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

	reader := c.newReader(topics)
	defer func() { _ = reader.Close() }()

	go c.cancelOnTopicChange(session, cancel, topics)

	for {
		message, err := reader.FetchMessage(session)
		if err == nil {
			err = c.handle(session, toKafkaMessage(message))
		}

		if err == nil {
			err = reader.CommitMessages(session, message)
		}

		if session.Err() != nil {
			return nil
		}

		if err != nil {
			return err
		}
	}
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

func toKafkaMessage(message kafka.Message) KafkaMessage {
	headers := make(map[string]string, len(message.Headers))
	for _, header := range message.Headers {
		headers[header.Key] = string(header.Value)
	}

	return KafkaMessage{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       message.Key,
		Value:     message.Value,
		Headers:   headers,
		Timestamp: message.Time,
	}
}
