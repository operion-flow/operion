package main

import (
	"context"
	"fmt"

	"github.com/dukex/operion/pkg/events"
)

// Event-driven trigger configuration handlers
// These handlers process domain events to configure individual trigger sources

// setupEventSubscriptions configures the event bus to receive trigger lifecycle events.
func (spm *ProviderManager) setupEventSubscriptions() error {
	// Subscribe to trigger lifecycle events
	if err := spm.eventBus.Handle(events.TriggerCreatedEventType, spm.handleTriggerCreatedEvent); err != nil {
		return fmt.Errorf("failed to subscribe to trigger.created events: %w", err)
	}

	if err := spm.eventBus.Handle(events.TriggerUpdatedEventType, spm.handleTriggerUpdatedEvent); err != nil {
		return fmt.Errorf("failed to subscribe to trigger.updated events: %w", err)
	}

	if err := spm.eventBus.Handle(events.TriggerDeletedEventType, spm.handleTriggerDeletedEvent); err != nil {
		return fmt.Errorf("failed to subscribe to trigger.deleted events: %w", err)
	}

	if err := spm.eventBus.Handle(events.WorkflowPublishedEventType, spm.handleWorkflowPublishedEvent); err != nil {
		return fmt.Errorf("failed to subscribe to workflow.published events: %w", err)
	}

	if err := spm.eventBus.Handle(events.WorkflowUnpublishedEventType, spm.handleWorkflowUnpublishedEvent); err != nil {
		return fmt.Errorf("failed to subscribe to workflow.unpublished events: %w", err)
	}

	spm.logger.Info("Event subscriptions configured successfully")

	return nil
}

// handleTriggerCreatedEvent processes trigger creation events.
func (spm *ProviderManager) handleTriggerCreatedEvent(ctx context.Context, eventData any) error {
	event, ok := eventData.(*events.TriggerCreatedEvent)
	if !ok {
		return fmt.Errorf("invalid event type for trigger.created: %T", eventData)
	}

	spm.logger.Info("Processing trigger created event",
		"trigger_id", event.TriggerID,
		"workflow_id", event.WorkflowID,
		"node_type", event.NodeType)

	// Transform domain event to provider configuration
	triggerConfig := spm.eventToTriggerConfig(event)

	// Validate configuration
	if err := spm.getTriggerConfigValidationErrors(triggerConfig); err != nil {
		spm.logger.Error("Invalid trigger configuration",
			"trigger_id", event.TriggerID,
			"error", err)

		return err
	}

	// Configure source only if business rules are satisfied
	if spm.shouldConfigureSource(triggerConfig) {
		return spm.configureSourceForTrigger(ctx, triggerConfig)
	}

	spm.logger.Info("Trigger created but not configuring source yet",
		"trigger_id", event.TriggerID,
		"reason", "business_rules_not_satisfied")

	return nil
}

// handleTriggerUpdatedEvent processes trigger update events.
func (spm *ProviderManager) handleTriggerUpdatedEvent(ctx context.Context, eventData any) error {
	event, ok := eventData.(*events.TriggerUpdatedEvent)
	if !ok {
		return fmt.Errorf("invalid event type for trigger.updated: %T", eventData)
	}

	spm.logger.Info("Processing trigger updated event",
		"trigger_id", event.TriggerID,
		"workflow_id", event.WorkflowID,
		"node_type", event.NodeType)

	// Transform domain event to provider configuration
	triggerConfig := spm.updatedEventToTriggerConfig(event)

	// Validate configuration
	if err := spm.getTriggerConfigValidationErrors(triggerConfig); err != nil {
		spm.logger.Error("Invalid trigger configuration",
			"trigger_id", event.TriggerID,
			"error", err)

		return err
	}

	// For updates, we reconfigure the trigger (which may involve removing and recreating)
	if spm.shouldConfigureSource(triggerConfig) {
		return spm.reconfigureSourceForTrigger(ctx, triggerConfig)
	}

	spm.logger.Info("Trigger updated but not reconfiguring source",
		"trigger_id", event.TriggerID,
		"reason", "business_rules_not_satisfied")

	return nil
}

// handleTriggerDeletedEvent processes trigger deletion events.
func (spm *ProviderManager) handleTriggerDeletedEvent(ctx context.Context, eventData any) error {
	event, ok := eventData.(*events.TriggerDeletedEvent)
	if !ok {
		return fmt.Errorf("invalid event type for trigger.deleted: %T", eventData)
	}

	spm.logger.Info("Processing trigger deleted event",
		"trigger_id", event.TriggerID,
		"workflow_id", event.WorkflowID,
		"node_type", event.NodeType,
		"source_id", event.SourceID)

	// Only remove source if it was actually configured (has source ID)
	if event.SourceID != "" {
		providerID := spm.extractProviderID(event.NodeType)

		return spm.removeSourceForTrigger(ctx, event.TriggerID, event.SourceID, providerID)
	}

	spm.logger.Info("Trigger deleted but no source to remove",
		"trigger_id", event.TriggerID,
		"reason", "no_source_id")

	return nil
}

// handleWorkflowPublishedEvent processes workflow publishing events.
func (spm *ProviderManager) handleWorkflowPublishedEvent(ctx context.Context, eventData any) error {
	event, ok := eventData.(*events.WorkflowPublishedEvent)
	if !ok {
		return fmt.Errorf("invalid event type for workflow.published: %T", eventData)
	}

	spm.logger.Info("Processing workflow published event",
		"workflow_id", event.WorkflowID,
		"workflow_name", event.WorkflowName,
		"trigger_count", len(event.TriggerNodes))

	// Configure sources for all triggers in the published workflow
	successCount := 0
	errorCount := 0

	for _, triggerNode := range event.TriggerNodes {
		triggerConfig := spm.nodeToTriggerConfig(event.WorkflowID, triggerNode)

		// Validate configuration
		if err := spm.getTriggerConfigValidationErrors(triggerConfig); err != nil {
			spm.logger.Error("Invalid trigger configuration in published workflow",
				"workflow_id", event.WorkflowID,
				"trigger_id", triggerConfig.TriggerID,
				"error", err)

			errorCount++

			continue
		}

		if err := spm.configureSourceForTrigger(ctx, triggerConfig); err != nil {
			spm.logger.Error("Failed to configure source for trigger in published workflow",
				"workflow_id", event.WorkflowID,
				"trigger_id", triggerConfig.TriggerID,
				"error", err)

			errorCount++

			continue
		}

		successCount++
	}

	spm.logger.Info("Completed workflow published event processing",
		"workflow_id", event.WorkflowID,
		"success_count", successCount,
		"error_count", errorCount)

	// Don't fail the entire workflow for individual trigger failures
	return nil
}

// handleWorkflowUnpublishedEvent processes workflow unpublishing events.
func (spm *ProviderManager) handleWorkflowUnpublishedEvent(ctx context.Context, eventData any) error {
	event, ok := eventData.(*events.WorkflowUnpublishedEvent)
	if !ok {
		return fmt.Errorf("invalid event type for workflow.unpublished: %T", eventData)
	}

	spm.logger.Info("Processing workflow unpublished event",
		"workflow_id", event.WorkflowID,
		"workflow_name", event.WorkflowName,
		"trigger_count", len(event.TriggerNodes))

	// Remove sources for all triggers in the unpublished workflow
	successCount := 0
	errorCount := 0

	for _, triggerNode := range event.TriggerNodes {
		if triggerNode.SourceID == "" {
			// Skip triggers that don't have sources configured
			continue
		}

		providerID := spm.extractProviderID(triggerNode.Type)
		if err := spm.removeSourceForTrigger(ctx, triggerNode.ID, triggerNode.SourceID, providerID); err != nil {
			spm.logger.Error("Failed to remove source for trigger in unpublished workflow",
				"workflow_id", event.WorkflowID,
				"trigger_id", triggerNode.ID,
				"source_id", triggerNode.SourceID,
				"error", err)

			errorCount++

			continue
		}

		successCount++
	}

	spm.logger.Info("Completed workflow unpublished event processing",
		"workflow_id", event.WorkflowID,
		"success_count", successCount,
		"error_count", errorCount)

	// Don't fail the entire workflow for individual trigger failures
	return nil
}
