package ingest_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/dukex/operion/pkg/ingest"
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

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	producer, err := sarama.NewSyncProducer(brokers, config)
	require.NoError(t, err)

	defer func() { _ = producer.Close() }()

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic:   employeesTopic,
		Key:     sarama.StringEncoder("emp-1"),
		Value:   sarama.StringEncoder(`{"name": "Ana"}`),
		Headers: []sarama.RecordHeader{{Key: []byte(sourceHeader), Value: []byte("hr")}},
	})
	require.NoError(t, err)

	group, err := sarama.NewConsumerGroup(brokers, "operion-test", config)
	require.NoError(t, err)

	defer func() { _ = group.Close() }()

	received := make(chan ingest.KafkaMessage, 1)
	consumerCtx, stop := context.WithCancel(ctx)

	defer stop()

	consumer := ingest.NewKafkaConsumer(group, always(employeesTopic), func(_ context.Context, message ingest.KafkaMessage) error {
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
