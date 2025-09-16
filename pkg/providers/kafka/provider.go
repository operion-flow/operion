package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/xeipuuv/gojsonschema"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	kafkaModels "github.com/dukex/operion/pkg/providers/kafka/models"
	kafkaPersistence "github.com/dukex/operion/pkg/providers/kafka/persistence"
)

// ConsumerManager manages a Kafka consumer and its associated sources.
type ConsumerManager struct {
	consumer          sarama.ConsumerGroup
	sources           map[string]*kafkaModels.KafkaSource // sourceID -> KafkaSource
	connectionDetails kafkaModels.ConnectionDetails
	consumerGroup     string
	cancel            context.CancelFunc
	logger            *slog.Logger
}

// KafkaProvider implements a centralized Kafka orchestrator that manages
// Kafka consumer groups and converts incoming messages to source events.
type KafkaProvider struct {
	config      map[string]any
	logger      *slog.Logger
	callback    protocol.SourceEventCallback
	persistence kafkaPersistence.KafkaPersistence
	consumers   map[string]*ConsumerManager // connectionDetailsID -> ConsumerManager
	started     bool
	mu          sync.RWMutex
}

// Start begins the centralized Kafka orchestrator.
func (k *KafkaProvider) Start(ctx context.Context, callback protocol.SourceEventCallback) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.started {
		return nil
	}

	k.callback = callback
	k.logger.Info("Starting centralized Kafka orchestrator")

	// Start all consumer managers
	for connectionDetailsID, consumerManager := range k.consumers {
		if err := k.startConsumerManager(ctx, consumerManager); err != nil {
			k.logger.Error("Failed to start consumer manager",
				"connection_details_id", connectionDetailsID,
				"error", err)

			return err
		}
	}

	k.started = true

	// Get source count from persistence for logging
	sources, err := k.persistence.ActiveKafkaSources()
	if err != nil {
		k.logger.Warn("Failed to get active sources count", "error", err)
		k.logger.Info("Centralized Kafka orchestrator started successfully")
	} else {
		k.logger.Info("Centralized Kafka orchestrator started successfully",
			"active_sources", len(sources),
			"consumer_managers", len(k.consumers))
	}

	return nil
}

// Stop gracefully shuts down the Kafka orchestrator.
func (k *KafkaProvider) Stop(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if !k.started {
		return nil
	}

	k.logger.Info("Stopping Kafka orchestrator")

	// Stop all consumer managers
	for connectionDetailsID, consumerManager := range k.consumers {
		if err := k.stopConsumerManager(consumerManager); err != nil {
			k.logger.Error("Error stopping consumer manager",
				"connection_details_id", connectionDetailsID,
				"error", err)
		}
	}

	k.started = false
	k.logger.Info("Kafka orchestrator stopped successfully")

	return nil
}

// Validate checks if the Kafka orchestrator configuration is valid.
func (k *KafkaProvider) Validate() error {
	// Orchestrator validation: ensure persistence is available
	if k.persistence == nil {
		return errors.New("kafka persistence not initialized")
	}

	return nil
}

// ProviderLifecycle interface implementation

// Initialize sets up the provider with required dependencies.
func (k *KafkaProvider) Initialize(ctx context.Context, deps protocol.Dependencies) error {
	k.logger = deps.Logger
	k.consumers = make(map[string]*ConsumerManager)

	// Initialize Kafka-specific persistence based on URL
	persistenceURL := os.Getenv("KAFKA_PERSISTENCE_URL")
	if persistenceURL == "" {
		return errors.New("kafka provider requires KAFKA_PERSISTENCE_URL environment variable (e.g., file://./data/kafka)")
	}

	persistence, err := k.createPersistence(ctx, persistenceURL)
	if err != nil {
		return err
	}

	k.persistence = persistence

	k.logger.Info("Kafka provider initialized", "persistence", persistenceURL)

	return nil
}

// Configure configures the provider based on current workflow definitions.
func (k *KafkaProvider) Configure(workflows []*models.Workflow) (map[string]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.logger.Info("Configuring Kafka provider with workflows", "workflow_count", len(workflows))

	triggerToSource := make(map[string]string)
	sourceCount := 0

	for _, wf := range workflows {
		if wf.Status != models.WorkflowStatusPublished {
			continue
		}

		// Filter trigger nodes with kafka provider
		for _, node := range wf.Nodes {
			if node.IsTriggerNode() && node.ProviderID != nil && *node.ProviderID == "kafka" {
				if sourceID := k.processKafkaTriggerNode(wf.ID, node); sourceID != "" {
					triggerToSource[node.ID] = sourceID
					sourceCount++
				}
			}
		}
	}

	// Create or update consumer managers based on configured sources
	if err := k.updateConsumerManagers(); err != nil {
		k.logger.Error("Failed to update consumer managers", "error", err)

		return nil, err
	}

	k.logger.Info("Kafka configuration completed",
		"created_sources", sourceCount,
		"consumer_managers", len(k.consumers))

	return triggerToSource, nil
}

// ConfigureTrigger configures an individual trigger with the provider.
func (k *KafkaProvider) ConfigureTrigger(ctx context.Context, trigger protocol.TriggerConfig) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.logger.Info("Configuring individual Kafka trigger",
		"trigger_id", trigger.TriggerID,
		"workflow_id", trigger.WorkflowID,
		"node_type", trigger.NodeType)

	// Create a temporary workflow node from the trigger config for compatibility with existing logic
	node := &models.WorkflowNode{
		ID:     trigger.TriggerID,
		Type:   trigger.NodeType,
		Config: trigger.Config,
	}

	// Process the Kafka trigger node to create/update source
	sourceID := k.processKafkaTriggerNode(trigger.WorkflowID, node)
	if sourceID == "" {
		return "", errors.New("failed to create/configure Kafka source for trigger")
	}

	// Update consumer managers to reflect the new source
	if err := k.updateConsumerManagers(); err != nil {
		k.logger.Error("Failed to update consumer managers after trigger configuration", "error", err)

		return sourceID, err // Return sourceID but also the error
	}

	// If the provider is already started, start any new consumer managers
	if k.started {
		for connectionDetailsID, consumerManager := range k.consumers {
			if consumerManager.consumer == nil { // New consumer manager that hasn't been started
				if err := k.startConsumerManager(ctx, consumerManager); err != nil {
					k.logger.Error("Failed to start new consumer manager",
						"connection_details_id", connectionDetailsID,
						"error", err)
				}
			}
		}
	}

	k.logger.Info("Successfully configured Kafka trigger",
		"trigger_id", trigger.TriggerID,
		"source_id", sourceID)

	return sourceID, nil
}

// RemoveTrigger removes an individual trigger from the provider.
func (k *KafkaProvider) RemoveTrigger(ctx context.Context, triggerID, sourceID string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.logger.Info("Removing Kafka trigger",
		"trigger_id", triggerID,
		"source_id", sourceID)

	if sourceID == "" {
		k.logger.Warn("No source ID provided for trigger removal", "trigger_id", triggerID)

		return nil // Nothing to remove
	}

	// Remove source from persistence
	if err := k.persistence.DeleteKafkaSource(sourceID); err != nil {
		k.logger.Error("Failed to remove Kafka source from persistence",
			"source_id", sourceID,
			"error", err)

		return err
	}

	// Update consumer managers to reflect the removed source
	if err := k.updateConsumerManagers(); err != nil {
		k.logger.Error("Failed to update consumer managers after trigger removal", "error", err)

		return err
	}

	k.logger.Info("Successfully removed Kafka trigger",
		"trigger_id", triggerID,
		"source_id", sourceID)

	return nil
}

// Prepare performs final preparation before starting the provider.
func (k *KafkaProvider) Prepare(ctx context.Context) error {
	if k.persistence == nil {
		return errors.New("kafka persistence not initialized")
	}

	k.logger.Info("Kafka provider prepared and ready")

	return nil
}

// processKafkaTriggerNode handles the creation of a Kafka source for a trigger node with Kafka type.
// Returns the sourceID if a source was successfully created, empty string otherwise.
func (k *KafkaProvider) processKafkaTriggerNode(workflowID string, node *models.WorkflowNode) string {
	sourceID := ""
	if node.SourceID != nil {
		sourceID = *node.SourceID
	}

	if sourceID == "" {
		// Generate a new UUID for the sourceID
		sourceID = uuid.New().String()
		k.logger.Info("Generated source_id for Kafka trigger node",
			"workflow_id", workflowID,
			"node_id", node.ID,
			"generated_source_id", sourceID)
	}

	// Check if source already exists by source ID
	existingSource, err := k.persistence.KafkaSourceByID(sourceID)
	if err != nil {
		k.logger.Error("Failed to check existing Kafka source",
			"source_id", sourceID,
			"error", err)

		return ""
	}

	if existingSource != nil {
		k.logger.Debug("Kafka source already exists", "source_id", sourceID)
		// Update configuration if needed
		if err := existingSource.UpdateConfiguration(node.Config); err != nil {
			k.logger.Error("Failed to update Kafka source configuration",
				"source_id", sourceID,
				"error", err)

			return ""
		}

		// Save updated source to persistence
		if err := k.persistence.SaveKafkaSource(existingSource); err != nil {
			k.logger.Error("Failed to update Kafka source in persistence",
				"source_id", sourceID,
				"error", err)
		}

		return sourceID // Return existing sourceID
	}

	// Create new Kafka source
	source, err := kafkaModels.NewKafkaSource(sourceID, node.Config)
	if err != nil {
		k.logger.Error("Failed to create Kafka source",
			"source_id", sourceID,
			"error", err)

		return ""
	}

	// Save source to persistence
	if err := k.persistence.SaveKafkaSource(source); err != nil {
		k.logger.Error("Failed to save Kafka source to persistence",
			"source_id", sourceID,
			"error", err)

		return ""
	}

	k.logger.Info("Created Kafka source",
		"source_id", sourceID,
		"connection_details_id", source.ConnectionDetailsID,
		"topic", source.ConnectionDetails.Topic,
		"consumer_group", source.GetConsumerGroup())

	return sourceID
}

// updateConsumerManagers creates or updates consumer managers based on active sources.
func (k *KafkaProvider) updateConsumerManagers() error {
	// Get all active sources from persistence
	sources, err := k.persistence.ActiveKafkaSources()
	if err != nil {
		return err
	}

	// Group sources by connection details ID
	sourcesByConnectionDetails := make(map[string][]*kafkaModels.KafkaSource)
	for _, source := range sources {
		sourcesByConnectionDetails[source.ConnectionDetailsID] = append(
			sourcesByConnectionDetails[source.ConnectionDetailsID],
			source,
		)
	}

	// Create or update consumer managers
	for connectionDetailsID, sources := range sourcesByConnectionDetails {
		if len(sources) == 0 {
			continue
		}

		// Use the first source's connection details (all sources with same ID have identical details)
		firstSource := sources[0]

		// Check if consumer manager already exists
		if existingManager, exists := k.consumers[connectionDetailsID]; exists {
			// Update existing consumer manager with new sources
			existingManager.sources = make(map[string]*kafkaModels.KafkaSource)
			for _, source := range sources {
				existingManager.sources[source.ID] = source
			}

			k.logger.Debug("Updated existing consumer manager",
				"connection_details_id", connectionDetailsID,
				"source_count", len(sources))
		} else {
			// Create new consumer manager
			manager := &ConsumerManager{
				sources:           make(map[string]*kafkaModels.KafkaSource),
				connectionDetails: firstSource.ConnectionDetails,
				consumerGroup:     firstSource.GetConsumerGroup(),
				logger: k.logger.With(
					"connection_details_id", connectionDetailsID,
					"consumer_group", firstSource.GetConsumerGroup(),
				),
			}

			for _, source := range sources {
				manager.sources[source.ID] = source
			}

			k.consumers[connectionDetailsID] = manager
			k.logger.Info("Created new consumer manager",
				"connection_details_id", connectionDetailsID,
				"consumer_group", manager.consumerGroup,
				"topic", manager.connectionDetails.Topic,
				"source_count", len(sources))
		}
	}

	// Remove consumer managers that are no longer needed
	for connectionDetailsID := range k.consumers {
		if _, exists := sourcesByConnectionDetails[connectionDetailsID]; !exists {
			if manager := k.consumers[connectionDetailsID]; manager != nil {
				if err := k.stopConsumerManager(manager); err != nil {
					k.logger.Error("Failed to stop unused consumer manager",
						"connection_details_id", connectionDetailsID,
						"error", err)
				}
			}

			delete(k.consumers, connectionDetailsID)
			k.logger.Info("Removed unused consumer manager",
				"connection_details_id", connectionDetailsID)
		}
	}

	return nil
}

// startConsumerManager starts a consumer manager.
func (k *KafkaProvider) startConsumerManager(ctx context.Context, manager *ConsumerManager) error {
	// Create Kafka consumer configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true

	// Create consumer group
	brokers := manager.connectionDetails.Brokers

	consumer, err := sarama.NewConsumerGroup(brokers, manager.consumerGroup, config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka consumer group: %w", err)
	}

	manager.consumer = consumer

	// Create context for this consumer
	consumerCtx, cancel := context.WithCancel(ctx)
	manager.cancel = cancel

	// Start consuming
	go k.runConsumerManager(consumerCtx, manager)

	// Monitor consumer errors
	go k.monitorConsumerErrors(consumerCtx, manager)

	manager.logger.Info("Consumer manager started successfully")

	return nil
}

// stopConsumerManager stops a consumer manager.
func (k *KafkaProvider) stopConsumerManager(manager *ConsumerManager) error {
	manager.logger.Info("Stopping consumer manager")

	// Cancel context
	if manager.cancel != nil {
		manager.cancel()
	}

	// Close consumer
	if manager.consumer != nil {
		if err := manager.consumer.Close(); err != nil {
			manager.logger.Error("Error closing Kafka consumer", "error", err)

			return err
		}
	}

	manager.logger.Info("Consumer manager stopped successfully")

	return nil
}

// runConsumerManager runs the consumer loop for a manager.
func (k *KafkaProvider) runConsumerManager(ctx context.Context, manager *ConsumerManager) {
	defer func() {
		if manager.consumer != nil {
			if err := manager.consumer.Close(); err != nil {
				manager.logger.Error("Error closing Kafka consumer", "error", err)
			}
		}
	}()

	handler := &kafkaConsumerGroupHandler{
		provider: k,
		manager:  manager,
	}

	for {
		select {
		case <-ctx.Done():
			manager.logger.Info("Consumer manager context cancelled")

			return
		default:
			err := manager.consumer.Consume(ctx, []string{manager.connectionDetails.Topic}, handler)
			if err != nil {
				manager.logger.Error("Kafka consumer error", "error", err)
				time.Sleep(5 * time.Second) // Retry after delay
			}
		}
	}
}

// monitorConsumerErrors monitors consumer errors.
func (k *KafkaProvider) monitorConsumerErrors(ctx context.Context, manager *ConsumerManager) {
	for {
		select {
		case err := <-manager.consumer.Errors():
			if err != nil {
				manager.logger.Error("Kafka consumer group error", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// kafkaConsumerGroupHandler implements sarama.ConsumerGroupHandler.
type kafkaConsumerGroupHandler struct {
	provider *KafkaProvider
	manager  *ConsumerManager
}

func (h *kafkaConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.manager.logger.Info("Kafka consumer group session started")

	return nil
}

func (h *kafkaConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.manager.logger.Info("Kafka consumer group session ended")

	return nil
}

func (h *kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	for message := range claim.Messages() {
		h.manager.logger.Debug("Received Kafka message",
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset)

		// Process message for all sources in this consumer manager
		for sourceID, source := range h.manager.sources {
			if err := h.processMessage(ctx, sourceID, source, message); err != nil {
				h.manager.logger.Error("Failed to process message for source",
					"source_id", sourceID,
					"error", err)
			}
		}

		// Mark message as processed
		session.MarkMessage(message, "")
	}

	return nil
}

// processMessage processes a Kafka message for a specific source.
func (h *kafkaConsumerGroupHandler) processMessage(ctx context.Context, sourceID string, source *kafkaModels.KafkaSource, message *sarama.ConsumerMessage) error {
	// Parse message data
	var (
		messageData any
		messageKey  string
	)

	if message.Key != nil {
		messageKey = string(message.Key)
	}

	// Try to parse message value as JSON
	if len(message.Value) > 0 {
		var jsonData any

		err := json.Unmarshal(message.Value, &jsonData)
		if err != nil {
			// If not JSON, store as raw string
			messageData = map[string]any{
				"raw_message": string(message.Value),
			}
		} else {
			messageData = jsonData
		}
	}

	// Validate against JSON schema if configured
	if source.HasJSONSchema() {
		if err := h.validateJSONSchema(messageData, source.JSONSchema); err != nil {
			h.manager.logger.Warn("Message failed JSON schema validation",
				"source_id", sourceID,
				"error", err)

			return nil // Discard invalid message as required by PRP
		}
	}

	// Parse headers
	headers := make(map[string]string)
	for _, header := range message.Headers {
		headers[string(header.Key)] = string(header.Value)
	}

	// Create event data following existing Kafka trigger format
	eventData := map[string]any{
		"topic":     message.Topic,
		"partition": message.Partition,
		"offset":    message.Offset,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"key":       messageKey,
		"message":   messageData,
		"headers":   headers,
	}

	// Publish source event
	return h.provider.callback(ctx, sourceID, "kafka", "message_received", eventData)
}

// validateJSONSchema validates message data against JSON schema.
func (h *kafkaConsumerGroupHandler) validateJSONSchema(messageData any, schema map[string]any) error {
	schemaLoader := gojsonschema.NewGoLoader(schema)
	dataLoader := gojsonschema.NewGoLoader(messageData)

	result, err := gojsonschema.Validate(schemaLoader, dataLoader)
	if err != nil {
		return err
	}

	if !result.Valid() {
		var errors []string
		for _, error := range result.Errors() {
			errors = append(errors, error.String())
		}

		return fmt.Errorf("JSON schema validation failed: %s", strings.Join(errors, "; "))
	}

	return nil
}

// createPersistence creates the appropriate persistence implementation based on URL scheme.
func (k *KafkaProvider) createPersistence(ctx context.Context, persistenceURL string) (kafkaPersistence.KafkaPersistence, error) {
	scheme := k.parsePersistenceScheme(persistenceURL)
	k.logger.Info("Initializing Kafka persistence", "scheme", scheme, "url", persistenceURL)

	switch scheme {
	case "file":
		// Extract path from file://path
		path := strings.TrimPrefix(persistenceURL, "file://")

		return kafkaPersistence.NewFilePersistence(path)
	case "postgres", "postgresql":
		// Create PostgreSQL persistence
		return kafkaPersistence.NewPostgresPersistence(ctx, k.logger, persistenceURL)
	case "mysql":
		// Future: implement database persistence
		return nil, errors.New("mysql persistence for Kafka not yet implemented")
	default:
		return nil, errors.New("unsupported persistence scheme: " + scheme + " (supported: file://)")
	}
}

// parsePersistenceScheme extracts the scheme from a persistence URL.
func (k *KafkaProvider) parsePersistenceScheme(persistenceURL string) string {
	parts := strings.SplitN(persistenceURL, "://", 2)
	if len(parts) < 2 {
		return "unknown"
	}

	return parts[0]
}
