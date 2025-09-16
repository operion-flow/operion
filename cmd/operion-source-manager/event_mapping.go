package main

import (
	"errors"
	"strings"

	"github.com/dukex/operion/pkg/events"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
)

// Event-to-Config mapping functions for transforming domain events
// into provider-specific trigger configurations

// eventToTriggerConfig transforms a TriggerCreatedEvent into a standardized TriggerConfig
// that can be used by any provider to configure a source.
func (spm *ProviderManager) eventToTriggerConfig(event *events.TriggerCreatedEvent) protocol.TriggerConfig {
	return protocol.TriggerConfig{
		TriggerID:  event.TriggerID,
		WorkflowID: event.WorkflowID,
		NodeType:   event.NodeType,
		Config:     event.Config,
		ProviderID: spm.extractProviderID(event.NodeType),
	}
}

// updatedEventToTriggerConfig transforms a TriggerUpdatedEvent into a standardized TriggerConfig.
func (spm *ProviderManager) updatedEventToTriggerConfig(event *events.TriggerUpdatedEvent) protocol.TriggerConfig {
	return protocol.TriggerConfig{
		TriggerID:  event.TriggerID,
		WorkflowID: event.WorkflowID,
		NodeType:   event.NodeType,
		Config:     event.Config,
		ProviderID: spm.extractProviderID(event.NodeType),
	}
}

// nodeToTriggerConfig transforms a workflow node into a standardized TriggerConfig.
// Used when processing WorkflowPublishedEvent that contains trigger nodes.
func (spm *ProviderManager) nodeToTriggerConfig(workflowID string, node events.TriggerNode) protocol.TriggerConfig {
	return protocol.TriggerConfig{
		TriggerID:  node.ID,
		WorkflowID: workflowID,
		NodeType:   node.Type,
		Config:     node.Config,
		ProviderID: spm.extractProviderID(node.Type),
	}
}

// workflowNodeToTriggerConfig transforms a models.WorkflowNode into a TriggerConfig.
// Used for backward compatibility during migration.
func (spm *ProviderManager) workflowNodeToTriggerConfig(workflowID string, node *models.WorkflowNode) protocol.TriggerConfig {
	return protocol.TriggerConfig{
		TriggerID:  node.ID,
		WorkflowID: workflowID,
		NodeType:   node.Type,
		Config:     node.Config,
		ProviderID: spm.extractProviderID(node.Type),
	}
}

// extractProviderID extracts the provider identifier from a node type.
// This mapping transforms node types into provider IDs for source configuration.
//
// Examples:
//   - "trigger:scheduler" -> "scheduler"
//   - "trigger:webhook" -> "webhook"
//   - "trigger:kafka" -> "kafka"
func (spm *ProviderManager) extractProviderID(nodeType string) string {
	// Extract provider from node type:
	// Node types follow the pattern "trigger:providerID"
	parts := strings.Split(nodeType, ":")
	if len(parts) >= 2 && parts[0] == "trigger" {
		return parts[1]
	}

	// Fallback: return the original nodeType if pattern doesn't match
	spm.logger.Warn("Unable to extract provider ID from node type, using nodeType as provider ID",
		"node_type", nodeType)

	return nodeType
}

// shouldConfigureSource determines if a trigger should be configured as a source
// based on business rules and workflow state.
func (spm *ProviderManager) shouldConfigureSource(triggerConfig protocol.TriggerConfig) bool {
	// Business rule: Only configure sources for published workflows
	// The activator handles workflow status filtering, so source manager
	// can defer this decision or implement its own workflow status checking

	// For now, configure all triggers that have valid provider mappings
	return spm.isProviderRunning(triggerConfig.ProviderID)
}

// isProviderRunning checks if the provider for this trigger is currently running.
func (spm *ProviderManager) isProviderRunning(providerID string) bool {
	spm.providerMutex.RLock()
	defer spm.providerMutex.RUnlock()

	_, exists := spm.runningProviders[providerID]

	return exists
}

// getTriggerConfigValidationErrors validates a TriggerConfig for completeness.
func (spm *ProviderManager) getTriggerConfigValidationErrors(config protocol.TriggerConfig) error {
	if config.TriggerID == "" {
		return &ConfigurationError{
			Type:    "validation_error",
			Message: "trigger_id is required",
			Details: map[string]any{"field": "trigger_id"},
		}
	}

	if config.WorkflowID == "" {
		return &ConfigurationError{
			Type:    "validation_error",
			Message: "workflow_id is required",
			Details: map[string]any{"field": "workflow_id"},
		}
	}

	if config.NodeType == "" {
		return &ConfigurationError{
			Type:    "validation_error",
			Message: "node_type is required",
			Details: map[string]any{"field": "node_type"},
		}
	}

	if config.ProviderID == "" {
		return &ConfigurationError{
			Type:    "validation_error",
			Message: "provider_id is required",
			Details: map[string]any{"field": "provider_id"},
		}
	}

	return nil
}

// ConfigurationError represents an error in trigger configuration.
type ConfigurationError struct {
	Type    string         `json:"type"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// Error returns the error message.
func (e *ConfigurationError) Error() string {
	return e.Message
}

// IsConfigurationError checks if an error is a ConfigurationError.
func IsConfigurationError(err error) bool {
	configurationError := &ConfigurationError{}
	ok := errors.As(err, &configurationError)

	return ok
}
