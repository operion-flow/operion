package ingest_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/ingest"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kafkaTc "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func TestKafkaConsumerReadsFromABroker(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker to start Kafka")
	}

	ctx := t.Context()

	container, err := kafkaTc.Run(ctx, "confluentinc/confluent-local:7.7.0", testcontainers.WithEnv(map[string]string{
		"KAFKA_CREATE_TOPICS": "true",
	}))
	require.NoError(t, err)

	defer func() { _ = container.Terminate(context.WithoutCancel(ctx)) }()

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), AllowAutoTopicCreation: true}

	defer func() { _ = writer.Close() }()

	require.Eventually(t, func() bool {
		return writer.WriteMessages(ctx, kafka.Message{
			Topic:   employeesTopic,
			Key:     []byte("emp-1"),
			Value:   []byte(`{"name": "Ana"}`),
			Headers: []kafka.Header{{Key: sourceHeader, Value: []byte("hr")}},
		}) == nil
	}, 30*time.Second, 500*time.Millisecond)

	received := make(chan ingest.KafkaMessage, 1)
	consumerCtx, stop := context.WithCancel(ctx)

	defer stop()

	newReader := func(topics []string) ingest.KafkaReader {
		return kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: "operion-test", GroupTopics: topics, StartOffset: kafka.FirstOffset})
	}

	consumer := ingest.NewKafkaConsumer(newReader, always(employeesTopic), func(_ context.Context, message ingest.KafkaMessage) error {
		received <- message

		return nil
	}, 100*time.Millisecond, slog.New(slog.DiscardHandler))

	go func() { _ = consumer.Run(consumerCtx) }()

	select {
	case message := <-received:
		assert.Equal(t, employeesTopic, message.Topic)
		assert.Equal(t, []byte("emp-1"), message.Key)
		assert.JSONEq(t, `{"name": "Ana"}`, string(message.Value))
		assert.Equal(t, map[string]string{sourceHeader: "hr"}, message.Headers)
	case <-time.After(60 * time.Second):
		t.Fatal("no message consumed from Kafka")
	}
}
