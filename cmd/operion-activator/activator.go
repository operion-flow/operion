package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dukex/operion/pkg/eventbus"
	"github.com/dukex/operion/pkg/events"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
)

// Activator consumes source events and triggers workflows based on registered triggers.
type Activator struct {
	id             string
	eventBus       eventbus.EventBus
	sourceEventBus eventbus.SourceEventBus
	persistence    persistence.Persistence
	logger         *slog.Logger
	restartCount   int
}

// NewActivator creates a new Activator instance.
func NewActivator(
	id string,
	persistence persistence.Persistence,
	eventBus eventbus.EventBus,
	sourceEventBus eventbus.SourceEventBus,
	logger *slog.Logger,
) *Activator {
	return &Activator{
		id:             id,
		eventBus:       eventBus,
		sourceEventBus: sourceEventBus,
		persistence:    persistence,
		logger:         logger.With("module", "activator"),
	}
}

// Start begins the activator service.
func (a *Activator) Start(ctx context.Context) {
	aCtx, cancel := context.WithCancel(ctx)

	a.logger.Info("Starting activator")

	a.handleSignals(aCtx, cancel)
	a.run(aCtx)
}

// handleSignals sets up signal handling for graceful shutdown and restart.
func (a *Activator) handleSignals(ctx context.Context, cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals
		a.logger.Info("Received signal", "signal", sig)

		switch sig {
		case syscall.SIGHUP:
			a.logger.Info("Reloading configuration...")
			a.restart(ctx, cancel)
		case syscall.SIGINT, syscall.SIGTERM:
			a.logger.Info("Shutting down gracefully...")
			a.stop(cancel)
			os.Exit(0)
		default:
			a.logger.Warn("Unhandled signal received", "signal", sig)
		}
	}()
}

// restart handles service restart with exponential backoff.
func (a *Activator) restart(ctx context.Context, cancel context.CancelFunc) {
	a.restartCount++
	newCtx := context.WithoutCancel(ctx)

	a.stop(cancel)

	if a.restartCount > 5 {
		a.logger.Error("Restart limit reached, exiting...")
		os.Exit(1)
	}

	backoff := time.Duration(a.restartCount) * time.Second
	a.logger.Info("Restarting activator...", "backoff", backoff)
	time.Sleep(backoff)

	a.Start(newCtx)
}

// run is the main loop that consumes source events.
func (a *Activator) run(ctx context.Context) {
	a.logger.Info("Starting source event consumption")

	// Set up source event subscription
	a.processSourceEvents(ctx)

	// Wait for context cancellation - the subscription runs in background goroutines
	<-ctx.Done()
	a.logger.Info("Activator context cancelled, stopping...")
}

// processSourceEvents handles incoming source events and triggers workflows.
func (a *Activator) processSourceEvents(ctx context.Context) {
	a.logger.Info("Setting up source event subscription")

	// Register handler for source events
	err := a.sourceEventBus.HandleSourceEvents(func(ctx context.Context, sourceEvent *events.SourceEvent) error {
		a.logger.Info("Received source event",
			"source_id", sourceEvent.SourceID,
			"provider_id", sourceEvent.ProviderID,
			"event_type", sourceEvent.EventType)

		return a.handleSourceEvent(ctx, sourceEvent)
	})
	if err != nil {
		a.logger.Error("Failed to register source event handler", "error", err)

		return
	}

	// Start subscribing to source events
	err = a.sourceEventBus.SubscribeToSourceEvents(ctx)
	if err != nil {
		a.logger.Error("Failed to start source event subscription", "error", err)

		return
	}

	a.logger.Info("Successfully subscribed to source events - waiting for events...")
}

// handleSourceEvent processes a single source event and triggers matching workflows.
func (a *Activator) handleSourceEvent(ctx context.Context, sourceEvent *events.SourceEvent) error {
	logger := a.logger.With(
		"source_id", sourceEvent.SourceID,
		"provider_id", sourceEvent.ProviderID,
		"event_type", sourceEvent.EventType,
	)

	logger.Info("Processing source event")

	// Validate the source event
	if err := sourceEvent.Validate(); err != nil {
		logger.Error("Invalid source event", "error", err)

		return err
	}

	// Find trigger nodes that match this source event
	matchingTriggerNodes, err := a.findTriggerNodesForSourceEvent(ctx, sourceEvent)
	if err != nil {
		logger.Error("Failed to find matching trigger nodes", "error", err)

		return err
	}

	logger.Info("Found matching trigger nodes", "count", len(matchingTriggerNodes))

	// Publish NodeActivation event for each matching trigger node
	for _, matchInfo := range matchingTriggerNodes {
		logger.Info("Will process trigger " + matchInfo.TriggerNode.ID)

		if err := a.publishNodeActivation(ctx, matchInfo.WorkflowID, matchInfo.TriggerNode.ID, sourceEvent.EventData); err != nil {
			logger.Error("Failed to publish NodeActivation event",
				"workflow_id", matchInfo.WorkflowID,
				"trigger_node_id", matchInfo.TriggerNode.ID,
				"error", err)
		}
	}

	return nil
}

// findTriggerNodesForSourceEvent queries the database for trigger nodes that match a source event.
func (a *Activator) findTriggerNodesForSourceEvent(ctx context.Context, sourceEvent *events.SourceEvent) ([]*models.TriggerNodeMatch, error) {
	// Use the node repository to find trigger nodes by source ID, event type, and provider ID
	matchingTriggerNodes, err := a.persistence.NodeRepository().FindTriggerNodesBySourceEventAndProvider(
		ctx,
		sourceEvent.SourceID,
		sourceEvent.EventType,
		sourceEvent.ProviderID,
		models.WorkflowStatusPublished,
	)
	if err != nil {
		a.logger.Error("Failed to fetch matching trigger nodes", "error", err)

		return nil, err
	}

	// No additional filtering needed - the persistence layer already filtered by all criteria
	return matchingTriggerNodes, nil
}

// publishNodeActivation publishes a NodeActivation event for a specific trigger node.
func (a *Activator) publishNodeActivation(ctx context.Context, workflowID, triggerNodeID string, sourceData map[string]any) error {
	logger := a.logger.With("workflow_id", workflowID, "trigger_node_id", triggerNodeID)
	logger.InfoContext(ctx, "Publishing NodeActivation event")

	// Generate execution ID for this workflow execution
	executionID := a.eventBus.GenerateID()
	logger.InfoContext(ctx, "Generated execution ID", "execution_id", executionID)

	// Load workflow to get variables
	workflow, err := a.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to load workflow", "error", err)

		return fmt.Errorf("failed to load workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		logger.ErrorContext(ctx, "Workflow not found", "workflowID", workflowID)

		return persistence.ErrWorkflowNotFound
	}

	// Copy variables from workflow, handle nil case
	var workflowVariables map[string]any
	if workflow.Variables != nil {
		workflowVariables = workflow.Variables
	} else {
		workflowVariables = make(map[string]any)
	}

	// Create execution context for this workflow execution
	executionCtx := &models.ExecutionContext{
		ID:          executionID,
		WorkflowID:  workflowID,
		Status:      models.ExecutionStatusRunning,
		NodeResults: make(map[string]models.NodeResult),
		TriggerData: sourceData,
		Variables:   workflowVariables,
		Metadata:    make(map[string]any),
		CreatedAt:   time.Now(),
	}

	// Save execution context before publishing the event
	logger.InfoContext(ctx, "Saving execution context", "execution_id", executionID)

	err = a.persistence.ExecutionContextRepository().SaveExecutionContext(ctx, executionCtx)
	if err != nil {
		logger.Error("Failed to save execution context", "error", err, "execution_id", executionID)

		return fmt.Errorf("failed to save execution context: %w", err)
	}

	logger.Info("Successfully saved execution context", "execution_id", executionID)

	event := events.NodeActivation{
		BaseEvent:   events.NewBaseEvent(events.NodeActivationEvent, workflowID),
		ExecutionID: executionID,
		NodeID:      triggerNodeID,
		WorkflowID:  workflowID,
		InputPort:   "external",
		InputData:   sourceData,
		SourceNode:  "", // External source
		SourcePort:  "", // External source
	}
	event.ID = a.eventBus.GenerateID()

	if err := a.eventBus.Publish(ctx, triggerNodeID+":"+executionID, event); err != nil {
		logger.Error("Failed to publish NodeActivation event", "error", err)

		return err
	}

	logger.With("event_id", event.ID, "execution_id", executionID).Info("Successfully published NodeActivation event")

	return nil
}

// stop gracefully shuts down the activator.
func (a *Activator) stop(cancel context.CancelFunc) {
	a.logger.Info("Stopping activator")

	if cancel != nil {
		cancel()
	}
}
