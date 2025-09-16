package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/dukex/operion/pkg/eventbus"
	"github.com/dukex/operion/pkg/events"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/protocol"
	"github.com/dukex/operion/pkg/registry"
)

type ProviderManager struct {
	id               string
	sourceEventBus   eventbus.SourceEventBus
	runningProviders map[string]protocol.Provider
	providerMutex    sync.RWMutex
	logger           *slog.Logger
	persistence      persistence.Persistence
	registry         *registry.Registry
	restartCount     int
	providerFilter   []string
}

func NewProviderManager(
	id string,
	persistence persistence.Persistence,
	sourceEventBus eventbus.SourceEventBus,
	logger *slog.Logger,
	registry *registry.Registry,
	providerFilter []string,
) *ProviderManager {
	return &ProviderManager{
		id:               id,
		logger:           logger.With("module", "operion-source-manager", "manager_id", id),
		persistence:      persistence,
		registry:         registry,
		restartCount:     0,
		sourceEventBus:   sourceEventBus,
		runningProviders: make(map[string]protocol.Provider),
		providerFilter:   providerFilter,
	}
}

func (spm *ProviderManager) Start(ctx context.Context) {
	spmCtx, cancel := context.WithCancel(ctx)

	spm.logger.Info("Starting source provider manager")

	spm.handleSignals(spmCtx, cancel)
	spm.run(ctx, cancel)
}

func (spm *ProviderManager) handleSignals(ctx context.Context, cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals
		spm.logger.Info("Received signal", "signal", sig)

		switch sig {
		case syscall.SIGHUP:
			spm.logger.Info("Reloading configuration...")
			spm.restart(ctx, cancel)
		case syscall.SIGINT, syscall.SIGTERM:
			spm.logger.Info("Shutting down gracefully...")
			spm.stop(ctx, cancel)
			os.Exit(0)
		default:
			spm.logger.Warn("Unhandled signal received", "signal", sig)
		}
	}()
}

func (spm *ProviderManager) restart(ctx context.Context, cancel context.CancelFunc) {
	spm.restartCount++
	spmCtx := context.WithoutCancel(ctx)
	spm.stop(spmCtx, cancel)

	if spm.restartCount > 5 {
		spm.logger.Error("Restart limit reached, exiting...")
		os.Exit(1)
	}

	backoff := time.Duration(spm.restartCount) * time.Second
	spm.logger.Info("Restarting source provider manager...", "backoff", backoff)
	time.Sleep(backoff)

	spm.Start(spmCtx)
}

func (spm *ProviderManager) run(ctx context.Context, cancel context.CancelFunc) {
	// Start source providers with lifecycle management
	if err := spm.startProviders(ctx); err != nil {
		spm.logger.Error("Failed to start source providers", "error", err)
		spm.restart(ctx, cancel)

		return
	}

	spm.logger.Info("Source provider manager started successfully")

	// Keep running until context is cancelled
	<-ctx.Done()

	spm.logger.Info("Source provider manager stopped")
}

func (spm *ProviderManager) startProviders(ctx context.Context) error {
	// Get available source providers from registry
	availableProviders := spm.registry.GetAvailableProviders()

	var providersToStart []protocol.ProviderFactory

	if len(spm.providerFilter) == 0 {
		// No filter - start all available providers
		providersToStart = availableProviders
	} else {
		// Filter providers based on configuration
		for _, factory := range availableProviders {
			if slices.Contains(spm.providerFilter, factory.ID()) {
				providersToStart = append(providersToStart, factory)
			}
		}
	}

	spm.logger.Info("Starting providers",
		"available_count", len(availableProviders),
		"filtered_count", len(providersToStart),
		"filter", spm.providerFilter)

	var wg sync.WaitGroup
	for _, factory := range providersToStart {
		wg.Add(1)

		go func(factory protocol.ProviderFactory) {
			defer wg.Done()

			if err := spm.startProvider(ctx, factory); err != nil {
				spm.logger.Error("Failed to start provider",
					"provider_id", factory.ID(),
					"error", err)
			}

			spm.logger.Info("Started provider", "provider_id", factory.ID())
		}(factory)
	}

	wg.Wait()

	return nil
}

func (spm *ProviderManager) startProvider(ctx context.Context, factory protocol.ProviderFactory) error {
	providerID := factory.ID()

	// Create default empty configuration - providers handle their own setup
	config := map[string]any{}
	sourceConfigs := []map[string]any{config}

	// Start a provider instance for each source configuration
	for _, config := range sourceConfigs {
		// Generate provider instance key using provider ID
		instanceKey := providerID

		provider, err := spm.registry.CreateProvider(ctx, providerID, config)
		if err != nil {
			spm.logger.Error("Failed to create source provider",
				"provider_id", providerID,
				"instance_key", instanceKey,
				"error", err)

			continue
		}

		// Execute complete lifecycle if provider supports it
		if lifecycle, ok := provider.(protocol.ProviderLifecycle); ok {
			if err := spm.executeProviderLifecycle(ctx, lifecycle, providerID, instanceKey); err != nil {
				continue
			}
		}

		spm.providerMutex.Lock()
		spm.runningProviders[instanceKey] = provider
		spm.providerMutex.Unlock()

		// Create callback for the provider
		callback := spm.createSourceEventCallback()

		// Step 4: Start the provider
		if err := provider.Start(ctx, callback); err != nil {
			spm.logger.Error("Failed to start source provider",
				"provider_id", providerID,
				"instance_key", instanceKey,
				"error", err)

			spm.providerMutex.Lock()
			delete(spm.runningProviders, instanceKey)
			spm.providerMutex.Unlock()

			continue
		}

		spm.logger.Info("Started source provider",
			"provider_id", providerID,
			"instance_key", instanceKey)
	}

	return nil
}

func (spm *ProviderManager) createSourceEventCallback() protocol.SourceEventCallback {
	return func(ctx context.Context, sourceID, providerType, eventType string, data map[string]any) error {
		logger := spm.logger.With(
			"source_id", sourceID,
			"provider_type", providerType,
			"event_type", eventType)

		logger.Info("Source event received, publishing to source event bus")

		// Create SourceEvent
		sourceEvent := events.NewSourceEvent(sourceID, providerType, eventType, data)

		// Validate the event
		if err := sourceEvent.Validate(); err != nil {
			logger.Error("Invalid source event", "error", err)

			return err
		}

		// Publish to source event bus
		logger.Info("Publishing source event",
			"source_id", sourceEvent.SourceID,
			"provider_id", sourceEvent.ProviderID,
			"event_type", sourceEvent.EventType)

		if err := spm.sourceEventBus.PublishSourceEvent(ctx, sourceEvent); err != nil {
			logger.Error("Failed to publish source event", "error", err)

			return err
		}

		logger.Info("Successfully published source event to event bus")

		return nil
	}
}

func (spm *ProviderManager) stop(ctx context.Context, cancel context.CancelFunc) {
	spm.logger.Info("Stopping source provider manager")

	if cancel != nil {
		cancel()
	}

	spm.providerMutex.Lock()
	defer spm.providerMutex.Unlock()

	for sourceID, provider := range spm.runningProviders {
		spm.logger.Info("Stopping source provider", "source_id", sourceID)

		if err := provider.Stop(ctx); err != nil {
			spm.logger.Error("Error stopping source provider",
				"source_id", sourceID,
				"error", err)
		}
	}

	spm.runningProviders = make(map[string]protocol.Provider)
	spm.logger.Info("All source providers stopped")
}

func (spm *ProviderManager) updateTriggersWithSourceIDs(ctx context.Context, triggerToSourceMap map[string]string) error {
	if len(triggerToSourceMap) == 0 {
		return nil
	}

	spm.logger.Info("Updating triggers with source IDs", "mapping_count", len(triggerToSourceMap))

	// Get all workflows to find and update triggers
	result, err := spm.persistence.WorkflowRepository().ListWorkflows(ctx, persistence.ListWorkflowsOptions{
		Limit:        1000, // Get all workflows for source ID management
		IncludeNodes: true, // Need nodes for trigger processing
	})
	if err != nil {
		return err
	}

	updated := 0

	for _, workflow := range result.Workflows {
		workflowUpdated, nodeUpdates := spm.updateWorkflowTriggerNodes(workflow, triggerToSourceMap)
		updated += nodeUpdates

		// Save workflow if any triggers were updated
		if workflowUpdated {
			if err := spm.persistence.WorkflowRepository().Save(ctx, workflow); err != nil {
				spm.logger.Error("Failed to save workflow with updated triggers",
					"workflow_id", workflow.ID,
					"error", err)

				return err
			}
		}
	}

	spm.logger.Info("Completed updating triggers with source IDs", "updated_count", updated)

	return nil
}

// updateWorkflowTriggerNodes updates trigger nodes in a workflow with source IDs and returns
// whether the workflow was updated and the count of nodes updated.
func (spm *ProviderManager) updateWorkflowTriggerNodes(workflow *models.Workflow, triggerToSourceMap map[string]string) (bool, int) {
	workflowUpdated := false
	updated := 0

	for _, node := range workflow.Nodes {
		if !node.IsTriggerNode() {
			continue
		}

		sourceID, exists := triggerToSourceMap[node.ID]
		if !exists {
			continue
		}

		currentSourceID := ""
		if node.SourceID != nil {
			currentSourceID = *node.SourceID
		}

		if currentSourceID == sourceID {
			continue
		}

		node.SourceID = &sourceID
		workflowUpdated = true
		updated++

		spm.logger.Info("Updated trigger node with source ID",
			"workflow_id", workflow.ID,
			"trigger_node_id", node.ID,
			"source_id", sourceID)
	}

	return workflowUpdated, updated
}

func (spm *ProviderManager) executeProviderLifecycle(ctx context.Context, lifecycle protocol.ProviderLifecycle, providerID, instanceKey string) error {
	deps := protocol.Dependencies{
		Logger: spm.logger,
	}

	// Step 1: Initialize dependencies
	if err := lifecycle.Initialize(ctx, deps); err != nil {
		spm.logger.Error("Failed to initialize provider",
			"provider_id", providerID,
			"instance_key", instanceKey,
			"error", err)

		return err
	}

	// Step 2: Configure with current workflows
	publishedStatus := models.WorkflowStatusPublished

	result, err := spm.persistence.WorkflowRepository().ListWorkflows(ctx, persistence.ListWorkflowsOptions{
		Status:       &publishedStatus, // Only get published workflows for source configuration
		Limit:        1000,             // Get all published workflows
		IncludeNodes: true,             // Need nodes for source configuration
	})
	if err != nil {
		spm.logger.Error("Failed to get workflows for configuration",
			"provider_id", providerID,
			"instance_key", instanceKey,
			"error", err)

		return err
	}

	// Configure provider and get triggerID -> sourceID mapping
	triggerToSourceMap, err := lifecycle.Configure(result.Workflows)
	if err != nil {
		spm.logger.Error("Failed to configure provider",
			"provider_id", providerID,
			"instance_key", instanceKey,
			"error", err)

		return err
	}

	// Update triggers with their sourceID mappings
	if err := spm.updateTriggersWithSourceIDs(ctx, triggerToSourceMap); err != nil {
		spm.logger.Error("Failed to update triggers with source IDs",
			"provider_id", providerID,
			"instance_key", instanceKey,
			"error", err)

		return err
	}

	// Step 3: Prepare for startup
	if err := lifecycle.Prepare(ctx); err != nil {
		spm.logger.Error("Failed to prepare provider",
			"provider_id", providerID,
			"instance_key", instanceKey,
			"error", err)

		return err
	}

	return nil
}
