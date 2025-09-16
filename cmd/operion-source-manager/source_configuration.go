package main

import (
	"context"
	"fmt"

	"github.com/dukex/operion/pkg/protocol"
)

// Source configuration methods for event-driven trigger management
// These methods handle the actual provider interactions for individual trigger configuration

// configureSourceForTrigger configures a single trigger source using the new individual configuration approach.
func (spm *ProviderManager) configureSourceForTrigger(ctx context.Context, triggerConfig protocol.TriggerConfig) error {
	// Get the appropriate provider using mapped provider ID
	provider, exists := spm.getRunningProvider(triggerConfig.ProviderID)
	if !exists {
		return &ConfigurationError{
			Type:    "provider_not_running",
			Message: fmt.Sprintf("provider %s not running", triggerConfig.ProviderID),
			Details: map[string]any{
				"provider_id": triggerConfig.ProviderID,
				"trigger_id":  triggerConfig.TriggerID,
			},
		}
	}

	// Check if provider supports individual trigger configuration
	lifecycle, ok := provider.(protocol.ProviderLifecycle)
	if !ok {
		return &ConfigurationError{
			Type:    "provider_lifecycle_not_supported",
			Message: fmt.Sprintf("provider %s does not implement ProviderLifecycle interface", triggerConfig.ProviderID),
			Details: map[string]any{
				"provider_id": triggerConfig.ProviderID,
				"trigger_id":  triggerConfig.TriggerID,
			},
		}
	}

	// Configure trigger in provider using standardized config
	sourceID, err := lifecycle.ConfigureTrigger(ctx, triggerConfig)
	if err != nil {
		return fmt.Errorf("failed to configure trigger %s in provider %s: %w",
			triggerConfig.TriggerID, triggerConfig.ProviderID, err)
	}

	spm.logger.Info("Successfully configured trigger source",
		"trigger_id", triggerConfig.TriggerID,
		"workflow_id", triggerConfig.WorkflowID,
		"provider_id", triggerConfig.ProviderID,
		"source_id", sourceID)

	// Update trigger node with source ID in database
	if err := spm.updateTriggerSourceID(ctx, triggerConfig.TriggerID, sourceID); err != nil {
		// Log error but don't fail - source is configured, just database update failed
		spm.logger.Error("Failed to update trigger with source ID - source configured but database not updated",
			"trigger_id", triggerConfig.TriggerID,
			"source_id", sourceID,
			"error", err)

		// Could implement rollback here if needed: lifecycle.RemoveTrigger(ctx, triggerConfig.TriggerID, sourceID)
	}

	return nil
}

// reconfigureSourceForTrigger handles trigger updates by reconfiguring the source.
func (spm *ProviderManager) reconfigureSourceForTrigger(ctx context.Context, triggerConfig protocol.TriggerConfig) error {
	// For now, use the same logic as configure - providers can handle updates internally
	// In a more sophisticated implementation, this could:
	// 1. Get existing source ID from database
	// 2. Call provider's RemoveTrigger if needed
	// 3. Call ConfigureTrigger with new configuration
	return spm.configureSourceForTrigger(ctx, triggerConfig)
}

// removeSourceForTrigger removes a trigger source configuration.
func (spm *ProviderManager) removeSourceForTrigger(ctx context.Context, triggerID, sourceID, providerID string) error {
	// Get the appropriate provider
	provider, exists := spm.getRunningProvider(providerID)
	if !exists {
		spm.logger.Warn("Provider not running during source removal - source may be orphaned",
			"provider_id", providerID,
			"trigger_id", triggerID,
			"source_id", sourceID)

		return nil // Don't fail - provider might be down temporarily
	}

	// Check if provider supports individual trigger configuration
	lifecycle, ok := provider.(protocol.ProviderLifecycle)
	if !ok {
		return &ConfigurationError{
			Type:    "provider_lifecycle_not_supported",
			Message: fmt.Sprintf("provider %s does not implement ProviderLifecycle interface", providerID),
			Details: map[string]any{
				"provider_id": providerID,
				"trigger_id":  triggerID,
				"source_id":   sourceID,
			},
		}
	}

	// Remove trigger from provider
	if err := lifecycle.RemoveTrigger(ctx, triggerID, sourceID); err != nil {
		return fmt.Errorf("failed to remove trigger %s from provider %s: %w",
			triggerID, providerID, err)
	}

	spm.logger.Info("Successfully removed trigger source",
		"trigger_id", triggerID,
		"provider_id", providerID,
		"source_id", sourceID)

	// Clear source ID from trigger node in database
	if err := spm.clearTriggerSourceID(ctx, triggerID); err != nil {
		spm.logger.Error("Failed to clear trigger source ID - source removed but database not updated",
			"trigger_id", triggerID,
			"source_id", sourceID,
			"error", err)
	}

	return nil
}

// getRunningProvider safely gets a running provider by ID.
func (spm *ProviderManager) getRunningProvider(providerID string) (protocol.Provider, bool) {
	spm.providerMutex.RLock()
	defer spm.providerMutex.RUnlock()

	provider, exists := spm.runningProviders[providerID]

	return provider, exists
}

// updateTriggerSourceID updates a trigger node with its assigned source ID.
func (spm *ProviderManager) updateTriggerSourceID(ctx context.Context, triggerID, sourceID string) error {
	// Create a single-item map for the existing update method
	triggerToSourceMap := map[string]string{
		triggerID: sourceID,
	}

	return spm.updateTriggersWithSourceIDs(ctx, triggerToSourceMap)
}

// clearTriggerSourceID removes the source ID from a trigger node.
func (spm *ProviderManager) clearTriggerSourceID(ctx context.Context, triggerID string) error {
	// This would need a new method in the persistence layer to clear source IDs
	// For now, we'll use the existing method with empty string (might not work)
	triggerToSourceMap := map[string]string{
		triggerID: "", // Clear the source ID
	}

	return spm.updateTriggersWithSourceIDs(ctx, triggerToSourceMap)
}
