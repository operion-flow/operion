package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	webhookModels "github.com/dukex/operion/pkg/providers/webhook/models"
	webhookPersistence "github.com/dukex/operion/pkg/providers/webhook/persistence"
)

// WebhookProvider implements a centralized webhook orchestrator that manages
// HTTP webhook endpoints and converts incoming requests to source events.
type WebhookProvider struct {
	config             map[string]any
	logger             *slog.Logger
	callback           protocol.SourceEventCallback
	server             *WebhookServer
	webhookPersistence webhookPersistence.WebhookPersistence
	port               int
	started            bool
	mu                 sync.RWMutex
}

// Start begins the centralized webhook orchestrator.
func (w *WebhookProvider) Start(ctx context.Context, callback protocol.SourceEventCallback) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.started {
		return nil
	}

	w.callback = callback
	w.logger.Info("Starting centralized webhook orchestrator", "port", w.port)

	// Set callback on server for event publishing
	w.server.SetCallback(callback)

	// Start HTTP server
	if err := w.server.Start(ctx); err != nil {
		return err
	}

	w.started = true

	// Get source count from persistence for logging
	sources, err := w.webhookPersistence.ActiveWebhookSources()
	if err != nil {
		w.logger.Warn("Failed to get active sources count", "error", err)
		w.logger.Info("Centralized webhook orchestrator started successfully", "port", w.port)
	} else {
		w.logger.Info("Centralized webhook orchestrator started successfully",
			"port", w.port,
			"active_sources", len(sources))
	}

	return nil
}

// Stop gracefully shuts down the webhook orchestrator.
func (w *WebhookProvider) Stop(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.started {
		return nil
	}

	w.logger.Info("Stopping webhook orchestrator")

	// Stop HTTP server
	if err := w.server.Stop(ctx); err != nil {
		w.logger.Error("Error stopping webhook server", "error", err)

		return err
	}

	w.started = false
	w.logger.Info("Webhook orchestrator stopped successfully")

	return nil
}

// Validate checks if the webhook orchestrator configuration is valid.
func (w *WebhookProvider) Validate() error {
	// Orchestrator validation: ensure server is available
	if w.server == nil {
		return errors.New("webhook server not initialized")
	}

	if w.port <= 0 || w.port > 65535 {
		return errors.New("invalid webhook server port")
	}

	// Orchestrator doesn't validate individual webhooks - those are validated when created
	return nil
}

// ProviderLifecycle interface implementation

// Initialize sets up the provider with required dependencies.
func (w *WebhookProvider) Initialize(ctx context.Context, deps protocol.Dependencies) error {
	w.logger = deps.Logger

	// Initialize webhook-specific persistence based on URL
	persistenceURL := os.Getenv("WEBHOOK_PERSISTENCE_URL")
	if persistenceURL == "" {
		return errors.New("webhook provider requires WEBHOOK_PERSISTENCE_URL environment variable (e.g., file://./data/webhook)")
	}

	persistence, err := w.createPersistence(ctx, persistenceURL)
	if err != nil {
		return err
	}

	w.webhookPersistence = persistence

	// Get webhook server port from environment or use default
	w.port = w.getWebhookPort()

	// Initialize webhook server
	w.server = NewWebhookServer(w.port, w.logger)
	w.server.SetPersistence(w.webhookPersistence)

	w.logger.Info("Webhook provider initialized", "port", w.port, "persistence", persistenceURL)

	return nil
}

// Configure configures the provider based on current workflow definitions.
func (w *WebhookProvider) Configure(workflows []*models.Workflow) (map[string]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logger.Info("Configuring webhook provider with workflows", "workflow_count", len(workflows))

	triggerToSource := make(map[string]string)
	sourceCount := 0

	for _, wf := range workflows {
		if wf.Status != models.WorkflowStatusPublished {
			continue
		}

		// Filter trigger nodes with webhook provider
		for _, node := range wf.Nodes {
			if node.IsTriggerNode() && node.ProviderID != nil && *node.ProviderID == "webhook" {
				if sourceID := w.processWebhookTriggerNode(wf.ID, node); sourceID != "" {
					triggerToSource[node.ID] = sourceID
					sourceCount++
				}
			}
		}
	}

	// Get total sources count from persistence for logging
	totalSources, err := w.webhookPersistence.WebhookSources()
	if err != nil {
		w.logger.Warn("Failed to get total sources count", "error", err)
		w.logger.Info("Webhook configuration completed", "created_sources", sourceCount)
	} else {
		w.logger.Info("Webhook configuration completed",
			"created_sources", sourceCount,
			"total_sources", len(totalSources))
	}

	return triggerToSource, nil
}

// ConfigureTrigger configures a single trigger source.
// Called when individual triggers are created or when workflows are published.
// Returns the sourceID assigned to this trigger.
func (w *WebhookProvider) ConfigureTrigger(ctx context.Context, trigger protocol.TriggerConfig) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logger.Info("Configuring individual webhook trigger",
		"trigger_id", trigger.TriggerID,
		"workflow_id", trigger.WorkflowID,
		"node_type", trigger.NodeType)

	// Validate that this is a webhook trigger
	if trigger.ProviderID != "webhook" {
		return "", fmt.Errorf("invalid provider ID for webhook trigger: %s", trigger.ProviderID)
	}

	// Process the trigger and create webhook source
	sourceID := w.processWebhookTriggerNode(trigger.WorkflowID, &models.WorkflowNode{
		ID:     trigger.TriggerID,
		Type:   trigger.NodeType,
		Config: trigger.Config,
	})

	if sourceID == "" {
		return "", fmt.Errorf("failed to create webhook source for trigger %s", trigger.TriggerID)
	}

	w.logger.Info("Successfully configured webhook trigger",
		"trigger_id", trigger.TriggerID,
		"source_id", sourceID)

	return sourceID, nil
}

// RemoveTrigger removes a trigger source configuration.
// Called when triggers are deleted or workflows are unpublished.
func (w *WebhookProvider) RemoveTrigger(ctx context.Context, triggerID, sourceID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logger.Info("Removing webhook trigger",
		"trigger_id", triggerID,
		"source_id", sourceID)

	// Delete the webhook source from persistence
	if err := w.webhookPersistence.DeleteWebhookSource(sourceID); err != nil {
		return fmt.Errorf("failed to delete webhook source %s for trigger %s: %w", sourceID, triggerID, err)
	}

	// Note: In a more complete implementation, we would unregister from the webhook server
	// For now, we just remove from persistence and the server will handle cleanup on restart

	w.logger.Info("Successfully removed webhook trigger",
		"trigger_id", triggerID,
		"source_id", sourceID)

	return nil
}

// Prepare performs final preparation before starting the provider.
func (w *WebhookProvider) Prepare(ctx context.Context) error {
	if w.server == nil {
		return errors.New("webhook server not initialized")
	}

	// Get all active sources from persistence and register with server
	sources, err := w.webhookPersistence.ActiveWebhookSources()
	if err != nil {
		return err
	}

	for _, source := range sources {
		if err := w.server.RegisterSource(source); err != nil {
			w.logger.Error("Failed to register webhook source",
				"source_id", source.ID,
				"external_id", source.ExternalID.String(),
				"error", err)

			return err
		}
	}

	w.logger.Info("Webhook provider prepared and ready",
		"registered_sources", len(sources))

	return nil
}

// processWebhookTriggerNode handles the creation of a webhook source for a trigger node with webhook type.
// Returns the sourceID if a source was successfully created, empty string otherwise.
func (w *WebhookProvider) processWebhookTriggerNode(workflowID string, node *models.WorkflowNode) string {
	sourceID := ""
	if node.SourceID != nil {
		sourceID = *node.SourceID
	}

	if sourceID == "" {
		// Generate a new UUID for the sourceID
		sourceID = uuid.New().String()
		w.logger.Info("Generated source_id for webhook trigger node",
			"workflow_id", workflowID,
			"node_id", node.ID,
			"generated_source_id", sourceID)
	}

	// Check if source already exists by source ID (not ExternalID)
	existingSource, err := w.webhookPersistence.WebhookSourceByID(sourceID)
	if err != nil {
		w.logger.Error("Failed to check existing webhook source",
			"source_id", sourceID,
			"error", err)

		return ""
	}

	if existingSource != nil {
		w.logger.Debug("Webhook source already exists", "source_id", sourceID)
		// Update configuration if needed
		existingSource.UpdateConfiguration(node.Config)

		// Save updated source to persistence
		if err := w.webhookPersistence.SaveWebhookSource(existingSource); err != nil {
			w.logger.Error("Failed to update webhook source in persistence",
				"source_id", sourceID,
				"error", err)
		}

		return sourceID // Return existing sourceID
	}

	// Create new webhook source
	source, err := webhookModels.NewWebhookSource(sourceID, node.Config)
	if err != nil {
		w.logger.Error("Failed to create webhook source",
			"source_id", sourceID,
			"error", err)

		return ""
	}

	// Save source to persistence
	if err := w.webhookPersistence.SaveWebhookSource(source); err != nil {
		w.logger.Error("Failed to save webhook source to persistence",
			"source_id", sourceID,
			"error", err)

		return ""
	}

	w.logger.Info("Created webhook source",
		"source_id", sourceID,
		"external_id", source.ExternalID.String(),
		"webhook_url", source.GetWebhookURL())

	return sourceID
}

// getWebhookPort gets the webhook server port from configuration or environment.
func (w *WebhookProvider) getWebhookPort() int {
	// Check configuration first
	if portVal, exists := w.config["port"]; exists {
		if port, ok := portVal.(int); ok && port > 0 && port <= 65535 {
			return port
		}

		if portStr, ok := portVal.(string); ok {
			if port, err := strconv.Atoi(portStr); err == nil && port > 0 && port <= 65535 {
				return port
			}
		}
	}

	// Check environment variable
	if portEnv := os.Getenv("WEBHOOK_PORT"); portEnv != "" {
		if port, err := strconv.Atoi(portEnv); err == nil && port > 0 && port <= 65535 {
			return port
		}
	}

	// Default port
	return 8085
}

// GetRegisteredSources returns registered sources from persistence for testing/debugging.
func (w *WebhookProvider) GetRegisteredSources() map[string]*webhookModels.WebhookSource {
	w.mu.RLock()
	defer w.mu.RUnlock()

	sources, err := w.webhookPersistence.WebhookSources()
	if err != nil {
		w.logger.Error("Failed to get sources from persistence", "error", err)

		return make(map[string]*webhookModels.WebhookSource)
	}

	// Convert slice to map with ExternalID as key
	result := make(map[string]*webhookModels.WebhookSource)
	for _, source := range sources {
		result[source.ExternalID.String()] = source
	}

	return result
}

// GetWebhookURL returns the webhook URL for a given source ID.
func (w *WebhookProvider) GetWebhookURL(sourceID string) string {
	source, err := w.webhookPersistence.WebhookSourceByID(sourceID)
	if err != nil || source == nil {
		return ""
	}

	return source.GetWebhookURL()
}

// createPersistence creates the appropriate persistence implementation based on URL scheme.
func (w *WebhookProvider) createPersistence(ctx context.Context, persistenceURL string) (webhookPersistence.WebhookPersistence, error) {
	scheme := w.parsePersistenceScheme(persistenceURL)
	w.logger.Info("Initializing webhook persistence", "scheme", scheme, "url", persistenceURL)

	switch scheme {
	case "file":
		// Extract path from file://path
		path := strings.TrimPrefix(persistenceURL, "file://")

		return webhookPersistence.NewFilePersistence(path)
	case "postgres", "postgresql":
		return webhookPersistence.NewPostgresPersistence(ctx, w.logger, persistenceURL)
	case "mysql":
		// Future: implement database persistence
		return nil, errors.New("mysql persistence for webhook not yet implemented")
	default:
		return nil, errors.New("unsupported persistence scheme: " + scheme + " (supported: file, postgres, mysql)")
	}
}

// parsePersistenceScheme extracts the scheme from a persistence URL.
func (w *WebhookProvider) parsePersistenceScheme(persistenceURL string) string {
	parts := strings.SplitN(persistenceURL, "://", 2)
	if len(parts) < 2 {
		return "unknown"
	}

	return parts[0]
}
