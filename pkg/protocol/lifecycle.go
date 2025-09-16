package protocol

import (
	"context"
	"log/slog"

	"github.com/dukex/operion/pkg/models"
)

// ProviderLifecycle defines the lifecycle management interface for source providers.
// This interface enables providers to handle their own initialization, configuration,
// and preparation phases before starting.
type ProviderLifecycle interface {
	// Initialize sets up the provider with required dependencies.
	// Called once when the source manager starts the provider.
	Initialize(ctx context.Context, deps Dependencies) error

	// ConfigureTrigger configures a single trigger source.
	// Called when individual triggers are created or when workflows are published.
	// Returns the sourceID assigned to this trigger.
	ConfigureTrigger(ctx context.Context, trigger TriggerConfig) (string, error)

	// RemoveTrigger removes a trigger source configuration.
	// Called when triggers are deleted or workflows are unpublished.
	RemoveTrigger(ctx context.Context, triggerID, sourceID string) error

	// Prepare performs final preparation before starting the provider.
	// Called after Initialize(), just before Start().
	Prepare(ctx context.Context) error

	// DEPRECATED: Configure configures the provider based on current workflow definitions.
	// This method is deprecated in favor of individual trigger configuration.
	// It will be removed in a future version once migration is complete.
	Configure(workflows []*models.Workflow) (map[string]string, error)
}

// TriggerConfig contains the configuration data for a single trigger source.
// This standardized structure is used by source manager to configure individual triggers.
type TriggerConfig struct {
	TriggerID  string         `json:"trigger_id"  validate:"required"` // Unique trigger node ID
	WorkflowID string         `json:"workflow_id" validate:"required"` // Workflow containing the trigger
	NodeType   string         `json:"node_type"   validate:"required"` // e.g., "trigger:scheduler"
	Config     map[string]any `json:"config"`                          // Node configuration from workflow
	ProviderID string         `json:"provider_id" validate:"required"` // e.g., "scheduler"
}

// Dependencies contains the common dependencies that providers need.
type Dependencies struct {
	Logger *slog.Logger
	// Note: No shared persistence - providers manage their own data
}
