//go:build integration
// +build integration

package kafka

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
)

// KafkaContainer represents a Kafka test container setup.
type KafkaContainer struct {
	kafkaContainer *kafka.KafkaContainer
	brokers        string
}

// setupKafkaContainer sets up a Kafka container using the official testcontainers Kafka module.
func setupKafkaContainer(t *testing.T) *KafkaContainer {
	ctx := context.Background()

	// Start Kafka using the official testcontainers Kafka module
	kafkaContainer, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster"),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.0"),
	)
	require.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	return &KafkaContainer{
		kafkaContainer: kafkaContainer,
		brokers:        brokers[0],
	}
}

// cleanup terminates the Kafka container setup.
func (kc *KafkaContainer) cleanup(t *testing.T) {
	ctx := context.Background()
	if kc.kafkaContainer != nil {
		err := kc.kafkaContainer.Terminate(ctx)
		assert.NoError(t, err)
	}
}

// createTopic creates a topic in the Kafka cluster.
func (kc *KafkaContainer) createTopic(t *testing.T, topic string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin([]string{kc.brokers}, config)
	require.NoError(t, err)
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topic, topicDetail, false)
	require.NoError(t, err)
}

// publishMessage publishes a message to a Kafka topic.
func (kc *KafkaContainer) publishMessage(t *testing.T, topic, key, message string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{kc.brokers}, config)
	require.NoError(t, err)
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)
	require.NoError(t, err)
}

func TestKafkaProvider_IntegrationWithRealKafka(t *testing.T) {
	// Setup Kafka container
	kafkaSetup := setupKafkaContainer(t)
	defer kafkaSetup.cleanup(t)

	// Create test topic
	testTopic := "integration-test-orders"
	kafkaSetup.createTopic(t, testTopic)

	// Set up persistence for test
	persistenceDir := t.TempDir()
	t.Setenv("KAFKA_PERSISTENCE_URL", "file://"+persistenceDir)

	// Create Kafka provider
	provider := &KafkaProvider{
		config: map[string]any{},
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	// Initialize provider
	ctx := context.Background()
	deps := protocol.Dependencies{
		Logger: provider.logger,
	}
	err := provider.Initialize(ctx, deps)
	require.NoError(t, err)

	// Create test workflow with Kafka trigger
	workflows := []*models.Workflow{
		createTestWorkflow("integration-workflow", []*models.WorkflowNode{
			createKafkaTriggerNode("integration-trigger", "integration-source", map[string]any{
				"topic":          testTopic,
				"brokers":        []string{kafkaSetup.brokers},
				"consumer_group": "operion-integration-test",
			}),
		}),
	}

	// Configure provider
	triggerToSource, err := provider.Configure(workflows)
	require.NoError(t, err)
	assert.Len(t, triggerToSource, 1)
	assert.Equal(t, "integration-source", triggerToSource["integration-trigger"])

	// Prepare provider
	err = provider.Prepare(ctx)
	require.NoError(t, err)

	// Channel to capture received events
	receivedEvents := make(chan map[string]any, 10)

	// Mock callback to capture events
	mockCallback := func(ctx context.Context, sourceID, providerID, eventType string, eventData map[string]any) error {
		assert.Equal(t, "integration-source", sourceID)
		assert.Equal(t, "kafka", providerID)
		assert.Equal(t, "message_received", eventType)
		receivedEvents <- eventData
		return nil
	}

	// Start provider
	err = provider.Start(ctx, mockCallback)
	require.NoError(t, err)

	// Wait for consumers to be ready
	time.Sleep(5 * time.Second)

	// Publish test messages
	testMessage1 := `{"order_id": "12345", "customer": "John Doe", "amount": 99.99}`
	testMessage2 := `{"order_id": "67890", "customer": "Jane Smith", "amount": 149.99}`

	kafkaSetup.publishMessage(t, testTopic, "order-key-1", testMessage1)
	kafkaSetup.publishMessage(t, testTopic, "order-key-2", testMessage2)

	// Wait for and verify first message
	select {
	case eventData := <-receivedEvents:
		assert.Equal(t, testTopic, eventData["topic"])
		assert.Equal(t, int32(0), eventData["partition"])
		assert.Equal(t, "order-key-1", eventData["key"])
		assert.NotEmpty(t, eventData["timestamp"])

		// Verify message data was parsed as JSON
		messageData, ok := eventData["message"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "12345", messageData["order_id"])
		assert.Equal(t, "John Doe", messageData["customer"])
		assert.Equal(t, 99.99, messageData["amount"])

	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for first message")
	}

	// Wait for and verify second message
	select {
	case eventData := <-receivedEvents:
		assert.Equal(t, testTopic, eventData["topic"])
		assert.Equal(t, "order-key-2", eventData["key"])

		// Verify message data
		messageData, ok := eventData["message"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "67890", messageData["order_id"])
		assert.Equal(t, "Jane Smith", messageData["customer"])
		assert.Equal(t, 149.99, messageData["amount"])

	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for second message")
	}

	// Stop provider
	err = provider.Stop(ctx)
	assert.NoError(t, err)
}

func TestKafkaProvider_IntegrationWithJSONSchemaValidation(t *testing.T) {
	// Setup Kafka container
	kafkaSetup := setupKafkaContainer(t)
	defer kafkaSetup.cleanup(t)

	// Create test topic
	testTopic := "integration-test-orders-validated"
	kafkaSetup.createTopic(t, testTopic)

	// Set up persistence for test
	persistenceDir := t.TempDir()
	t.Setenv("KAFKA_PERSISTENCE_URL", "file://"+persistenceDir)

	// Create Kafka provider
	provider := &KafkaProvider{
		config: map[string]any{},
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	// Initialize provider
	ctx := context.Background()
	deps := protocol.Dependencies{
		Logger: provider.logger,
	}
	err := provider.Initialize(ctx, deps)
	require.NoError(t, err)

	// Create test workflow with Kafka trigger and JSON schema
	workflows := []*models.Workflow{
		createTestWorkflow("validation-workflow", []*models.WorkflowNode{
			createKafkaTriggerNode("validation-trigger", "validation-source", map[string]any{
				"topic":          testTopic,
				"brokers":        []string{kafkaSetup.brokers},
				"consumer_group": "operion-validation-test",
				"json_schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"order_id": map[string]any{"type": "string"},
						"amount":   map[string]any{"type": "number"},
					},
					"required": []string{"order_id", "amount"},
				},
			}),
		}),
	}

	// Configure and prepare provider
	_, err = provider.Configure(workflows)
	require.NoError(t, err)
	err = provider.Prepare(ctx)
	require.NoError(t, err)

	// Channel to capture received events
	receivedEvents := make(chan map[string]any, 10)

	// Mock callback to capture valid events
	mockCallback := func(ctx context.Context, sourceID, providerID, eventType string, eventData map[string]any) error {
		receivedEvents <- eventData
		return nil
	}

	// Start provider
	err = provider.Start(ctx, mockCallback)
	require.NoError(t, err)

	// Wait for consumers to be ready
	time.Sleep(5 * time.Second)

	// Publish valid and invalid messages
	validMessage := `{"order_id": "12345", "amount": 99.99, "customer": "John Doe"}`
	invalidMessage1 := `{"amount": 99.99}`                          // Missing required order_id
	invalidMessage2 := `{"order_id": "67890", "amount": "invalid"}` // Invalid amount type

	kafkaSetup.publishMessage(t, testTopic, "valid-key", validMessage)
	kafkaSetup.publishMessage(t, testTopic, "invalid-key-1", invalidMessage1)
	kafkaSetup.publishMessage(t, testTopic, "invalid-key-2", invalidMessage2)

	// Should only receive the valid message
	select {
	case eventData := <-receivedEvents:
		assert.Equal(t, "valid-key", eventData["key"])

		// Verify message data
		messageData, ok := eventData["message"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "12345", messageData["order_id"])
		assert.Equal(t, 99.99, messageData["amount"])

	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for valid message")
	}

	// Should not receive any more messages (invalid ones should be discarded)
	select {
	case eventData := <-receivedEvents:
		t.Fatalf("Unexpected event received for invalid message: %+v", eventData)
	case <-time.After(5 * time.Second):
		// Expected - no more messages should be received
	}

	// Stop provider
	err = provider.Stop(ctx)
	assert.NoError(t, err)
}

func TestKafkaProvider_IntegrationConsumerSharing(t *testing.T) {
	// Setup Kafka container
	kafkaSetup := setupKafkaContainer(t)
	defer kafkaSetup.cleanup(t)

	// Create test topics
	testTopic1 := "integration-shared-topic"
	testTopic2 := "integration-different-topic"
	kafkaSetup.createTopic(t, testTopic1)
	kafkaSetup.createTopic(t, testTopic2)

	// Set up persistence for test
	persistenceDir := t.TempDir()
	t.Setenv("KAFKA_PERSISTENCE_URL", "file://"+persistenceDir)

	// Create Kafka provider
	provider := &KafkaProvider{
		config: map[string]any{},
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	// Initialize provider
	ctx := context.Background()
	deps := protocol.Dependencies{
		Logger: provider.logger,
	}
	err := provider.Initialize(ctx, deps)
	require.NoError(t, err)

	// Create test workflows with sources that should share consumers
	workflows := []*models.Workflow{
		createTestWorkflow("shared-workflow-1", []*models.WorkflowNode{
			// These two sources should share the same consumer (same connection details)
			createKafkaTriggerNode("shared-trigger-1", "shared-source-1", map[string]any{
				"topic":          testTopic1,
				"brokers":        []string{kafkaSetup.brokers},
				"consumer_group": "operion-shared-test",
			}),
			createKafkaTriggerNode("shared-trigger-2", "shared-source-2", map[string]any{
				"topic":          testTopic1,
				"brokers":        []string{kafkaSetup.brokers},
				"consumer_group": "operion-shared-test",
			}),
		}),
		createTestWorkflow("different-workflow", []*models.WorkflowNode{
			// This source should have a different consumer (different topic)
			createKafkaTriggerNode("different-trigger", "different-source", map[string]any{
				"topic":          testTopic2,
				"brokers":        []string{kafkaSetup.brokers},
				"consumer_group": "operion-different-test",
			}),
		}),
	}

	// Configure provider
	_, err = provider.Configure(workflows)
	require.NoError(t, err)

	// Verify consumer managers were created correctly
	assert.Len(t, provider.consumers, 2) // Two different connection details should create 2 managers

	// Find the consumer managers
	var sharedConsumerManager, differentConsumerManager *ConsumerManager
	for _, manager := range provider.consumers {
		if manager.connectionDetails.Topic == testTopic1 {
			sharedConsumerManager = manager
		} else if manager.connectionDetails.Topic == testTopic2 {
			differentConsumerManager = manager
		}
	}

	require.NotNil(t, sharedConsumerManager)
	require.NotNil(t, differentConsumerManager)

	// Verify shared consumer manager has 2 sources
	assert.Len(t, sharedConsumerManager.sources, 2)
	assert.Contains(t, sharedConsumerManager.sources, "shared-source-1")
	assert.Contains(t, sharedConsumerManager.sources, "shared-source-2")

	// Verify different consumer manager has 1 source
	assert.Len(t, differentConsumerManager.sources, 1)
	assert.Contains(t, differentConsumerManager.sources, "different-source")

	// Verify consumer groups are correct
	assert.Equal(t, "operion-shared-test", sharedConsumerManager.consumerGroup)
	assert.Equal(t, "operion-different-test", differentConsumerManager.consumerGroup)

	// Prepare and start provider for message testing
	err = provider.Prepare(ctx)
	require.NoError(t, err)

	// Channel to capture received events
	receivedEvents := make(chan map[string]any, 10)

	// Mock callback to capture events
	mockCallback := func(ctx context.Context, sourceID, providerID, eventType string, eventData map[string]any) error {
		event := map[string]any{
			"source_id":  sourceID,
			"event_data": eventData,
		}
		receivedEvents <- event
		return nil
	}

	// Start provider
	err = provider.Start(ctx, mockCallback)
	require.NoError(t, err)

	// Wait for consumers to be ready
	time.Sleep(5 * time.Second)

	// Publish messages to both topics
	kafkaSetup.publishMessage(t, testTopic1, "shared-key", `{"message": "shared topic"}`)
	kafkaSetup.publishMessage(t, testTopic2, "different-key", `{"message": "different topic"}`)

	// Should receive events for both sources sharing the first topic
	receivedSourceIDs := make(map[string]bool)
	eventsReceived := 0

	for eventsReceived < 3 { // Expecting 3 events: 2 for shared topic + 1 for different topic
		select {
		case event := <-receivedEvents:
			sourceID := event["source_id"].(string)
			eventData := event["event_data"].(map[string]any)
			receivedSourceIDs[sourceID] = true

			if sourceID == "shared-source-1" || sourceID == "shared-source-2" {
				assert.Equal(t, testTopic1, eventData["topic"])
				assert.Equal(t, "shared-key", eventData["key"])
			} else if sourceID == "different-source" {
				assert.Equal(t, testTopic2, eventData["topic"])
				assert.Equal(t, "different-key", eventData["key"])
			}

			eventsReceived++

		case <-time.After(30 * time.Second):
			t.Fatalf("Timeout waiting for events. Received %d events, expected 3", eventsReceived)
		}
	}

	// Verify all sources received events
	assert.True(t, receivedSourceIDs["shared-source-1"])
	assert.True(t, receivedSourceIDs["shared-source-2"])
	assert.True(t, receivedSourceIDs["different-source"])

	// Stop provider
	err = provider.Stop(ctx)
	assert.NoError(t, err)
}
