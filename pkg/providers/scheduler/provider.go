package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/dukex/operion/pkg/events"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	schedulerModels "github.com/dukex/operion/pkg/providers/scheduler/models"
	schedulerPersistence "github.com/dukex/operion/pkg/providers/scheduler/persistence"
)

const (
	schedulerProviderID = "scheduler"
)

// SchedulerProvider implements a centralized cron-based scheduler orchestrator
// that polls the database for due schedules and processes them regardless of their individual cron expressions.
type SchedulerProvider struct {
	config               map[string]any
	logger               *slog.Logger
	schedulerPersistence schedulerPersistence.SchedulerPersistence
	callback             protocol.SourceEventCallback
	ticker               *time.Ticker
	done                 chan bool
	started              bool
	mu                   sync.RWMutex
}

// Start begins the centralized scheduler orchestrator.
func (s *SchedulerProvider) Start(ctx context.Context, callback protocol.SourceEventCallback) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	s.callback = callback
	s.logger.Info("Starting centralized scheduler orchestrator")

	// Start centralized poller (runs every minute to check all due schedules)
	s.ticker = time.NewTicker(1 * time.Minute)
	s.done = make(chan bool)
	s.started = true

	go s.pollSchedules(ctx)

	s.logger.Info("Centralized scheduler orchestrator started successfully")

	return nil
}

// Stop gracefully shuts down the scheduler orchestrator.
func (s *SchedulerProvider) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	s.logger.Info("Stopping scheduler orchestrator")

	if s.ticker != nil {
		s.ticker.Stop()
	}

	select {
	case s.done <- true:
	default:
	}

	s.started = false
	s.logger.Info("Scheduler orchestrator stopped successfully")

	return nil
}

// Validate checks if the scheduler orchestrator configuration is valid.
func (s *SchedulerProvider) Validate() error {
	// Orchestrator validation: ensure persistence is available
	if s.schedulerPersistence == nil {
		return events.ErrInvalidEventData
	}

	// Orchestrator doesn't validate individual schedules - those are validated when created
	// Each schedule in the database has its own cron expression validation
	return nil
}

// pollSchedules is the centralized poller that runs every minute.
func (s *SchedulerProvider) pollSchedules(ctx context.Context) {
	for {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		case <-s.ticker.C:
			s.processDueSchedules(ctx)
		}
	}
}

// processDueSchedules queries database for ALL due schedules and publishes events
// This is the core orchestrator method that handles schedules with different cron expressions.
func (s *SchedulerProvider) processDueSchedules(ctx context.Context) {
	now := time.Now().UTC()

	// Query database for ALL schedules that are due, regardless of cron expression
	dueSchedules, err := s.getDueSchedules(now)
	if err != nil {
		s.logger.Error("Failed to get due schedules", "error", err)

		return
	}

	if len(dueSchedules) > 0 {
		s.logger.Info("Processing due schedules", "count", len(dueSchedules))
	}

	for _, schedule := range dueSchedules {
		s.logger.Info("Processing due schedule",
			"source_id", schedule.SourceID,
			"cron_expression", schedule.CronExpression,
			"due_at", schedule.NextDueAt)

		// Publish source event (includes schedule's own cron expression)
		if err := s.publishScheduleEvent(ctx, schedule); err != nil {
			s.logger.Error("Failed to publish schedule event",
				"source_id", schedule.SourceID,
				"error", err)

			continue
		}

		// Update next execution time using schedule's own cron expression
		if err := schedule.UpdateNextDueAt(); err != nil {
			s.logger.Error("Failed to update next due at",
				"source_id", schedule.SourceID,
				"error", err)

			continue
		}

		// Save updated schedule back to database
		if err := s.updateSchedule(schedule); err != nil {
			s.logger.Error("Failed to update schedule",
				"source_id", schedule.SourceID,
				"error", err)
		}
	}
}

// getDueSchedules retrieves schedules that are due for execution.
func (s *SchedulerProvider) getDueSchedules(now time.Time) ([]*schedulerModels.Schedule, error) {
	return s.schedulerPersistence.DueSchedules(now)
}

// updateSchedule saves the updated schedule back to the database.
func (s *SchedulerProvider) updateSchedule(schedule *schedulerModels.Schedule) error {
	if err := s.schedulerPersistence.SaveSchedule(schedule); err != nil {
		return err
	}

	s.logger.Info("Schedule updated",
		"source_id", schedule.SourceID,
		"next_due_at", schedule.NextDueAt)

	return nil
}

// publishScheduleEvent publishes a source event for a due schedule.
func (s *SchedulerProvider) publishScheduleEvent(ctx context.Context, schedule *schedulerModels.Schedule) error {
	now := time.Now()

	eventData := map[string]any{
		"cron_expression": schedule.CronExpression,
		"due_at":          schedule.NextDueAt.Format("2006-01-02 15:04"),
		"published_at":    now.Format("2006-01-02 15:04:05.000"),
	}

	return s.callback(ctx, schedule.SourceID, schedulerProviderID, "schedule_due", eventData)
}

// ProviderLifecycle interface implementation

// Initialize sets up the provider with required dependencies.
func (s *SchedulerProvider) Initialize(ctx context.Context, deps protocol.Dependencies) error {
	s.logger = deps.Logger

	// Initialize scheduler-specific persistence based on URL
	persistenceURL := os.Getenv("SCHEDULER_PERSISTENCE_URL")
	if persistenceURL == "" {
		return errors.New("scheduler provider requires SCHEDULER_PERSISTENCE_URL environment variable (e.g., file://./data/scheduler, postgres://...)")
	}

	persistence, err := s.createPersistence(ctx, persistenceURL)
	if err != nil {
		return err
	}

	s.schedulerPersistence = persistence

	return nil
}

// Configure configures the provider based on current workflow definitions.
func (s *SchedulerProvider) Configure(workflows []*models.Workflow) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Configuring scheduler provider with workflows", "workflow_count", len(workflows))

	triggerToSource := make(map[string]string)
	scheduleCount := 0

	for _, wf := range workflows {
		if wf.Status != models.WorkflowStatusPublished {
			continue
		}

		// Filter trigger nodes with scheduler provider
		for _, node := range wf.Nodes {
			if node.IsTriggerNode() && node.ProviderID != nil && *node.ProviderID == schedulerProviderID {
				if cronExpr, exists := node.Config["cron_expression"]; exists {
					if sourceID := s.processScheduleTriggerNode(wf.ID, node, cronExpr); sourceID != "" {
						triggerToSource[node.ID] = sourceID
						scheduleCount++
					}
				}
			}
		}
	}

	s.logger.Info("Scheduler configuration completed", "created_schedules", scheduleCount)

	return triggerToSource, nil
}

// ConfigureTrigger configures a single trigger source.
// Called when individual triggers are created or when workflows are published.
// Returns the sourceID assigned to this trigger.
func (s *SchedulerProvider) ConfigureTrigger(ctx context.Context, trigger protocol.TriggerConfig) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Configuring individual scheduler trigger",
		"trigger_id", trigger.TriggerID,
		"workflow_id", trigger.WorkflowID,
		"node_type", trigger.NodeType)

	// Validate that this is a scheduler trigger
	if trigger.ProviderID != schedulerProviderID {
		return "", fmt.Errorf("invalid provider ID for scheduler trigger: %s", trigger.ProviderID)
	}

	// Extract cron expression from config
	cronExpr, exists := trigger.Config["cron_expression"]
	if !exists {
		return "", fmt.Errorf("cron_expression is required for scheduler trigger %s", trigger.TriggerID)
	}

	// Process the trigger and create schedule
	sourceID := s.processScheduleTriggerNode(trigger.WorkflowID, &models.WorkflowNode{
		ID:     trigger.TriggerID,
		Type:   trigger.NodeType,
		Config: trigger.Config,
	}, cronExpr)

	if sourceID == "" {
		return "", fmt.Errorf("failed to create schedule for trigger %s", trigger.TriggerID)
	}

	s.logger.Info("Successfully configured scheduler trigger",
		"trigger_id", trigger.TriggerID,
		"source_id", sourceID,
		"cron_expression", cronExpr)

	return sourceID, nil
}

// RemoveTrigger removes a trigger source configuration.
// Called when triggers are deleted or workflows are unpublished.
func (s *SchedulerProvider) RemoveTrigger(ctx context.Context, triggerID, sourceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Removing scheduler trigger",
		"trigger_id", triggerID,
		"source_id", sourceID)

	// Delete the schedule from persistence
	if err := s.schedulerPersistence.DeleteSchedule(sourceID); err != nil {
		return fmt.Errorf("failed to delete schedule %s for trigger %s: %w", sourceID, triggerID, err)
	}

	s.logger.Info("Successfully removed scheduler trigger",
		"trigger_id", triggerID,
		"source_id", sourceID)

	return nil
}

// Prepare performs final preparation before starting the provider.
func (s *SchedulerProvider) Prepare(ctx context.Context) error {
	if s.schedulerPersistence == nil {
		return errors.New("scheduler persistence not initialized")
	}

	s.logger.Info("Scheduler provider prepared and ready")

	return nil
}

// processScheduleTriggerNode handles the creation of a schedule for a trigger node with cron_expression.
// Returns the sourceID if a schedule was successfully created, empty string otherwise.
func (s *SchedulerProvider) processScheduleTriggerNode(workflowID string, node *models.WorkflowNode, cronExpr any) string {
	sourceID := ""
	if node.SourceID != nil {
		sourceID = *node.SourceID
	}

	if sourceID == "" {
		// Generate a new UUID for the sourceID
		sourceID = uuid.New().String()
		s.logger.Info("Generated source_id for scheduler trigger node",
			"workflow_id", workflowID,
			"node_id", node.ID,
			"generated_source_id", sourceID)
	}

	// Check if schedule already exists
	existingSchedule, err := s.schedulerPersistence.ScheduleBySourceID(sourceID)
	if err != nil {
		s.logger.Error("Failed to check existing schedule",
			"source_id", sourceID,
			"error", err)

		return ""
	}

	if existingSchedule != nil {
		s.logger.Debug("Schedule already exists", "source_id", sourceID)

		return sourceID // Return existing sourceID
	}

	// Create new schedule
	cronStr, ok := cronExpr.(string)
	if !ok {
		s.logger.Warn("Invalid cron_expression type",
			"source_id", sourceID,
			"type", cronExpr)

		return ""
	}

	schedule, err := schedulerModels.NewSchedule(sourceID, sourceID, cronStr)
	if err != nil {
		s.logger.Error("Failed to create schedule",
			"source_id", sourceID,
			"cron", cronStr,
			"error", err)

		return ""
	}

	if err := s.schedulerPersistence.SaveSchedule(schedule); err != nil {
		s.logger.Error("Failed to save schedule",
			"source_id", sourceID,
			"error", err)

		return ""
	}

	s.logger.Info("Created schedule",
		"source_id", sourceID,
		"cron", cronStr,
		"next_due_at", schedule.NextDueAt)

	return sourceID
}

// createPersistence creates the appropriate persistence implementation based on URL scheme.
func (s *SchedulerProvider) createPersistence(ctx context.Context, persistenceURL string) (schedulerPersistence.SchedulerPersistence, error) {
	scheme := s.parsePersistenceScheme(persistenceURL)

	s.logger.Info("Initializing scheduler persistence", "scheme", scheme, "url", persistenceURL)

	switch scheme {
	case "file":
		// Extract path from file://path
		path := strings.TrimPrefix(persistenceURL, "file://")

		return schedulerPersistence.NewFilePersistence(path)

	case "postgres", "postgresql":
		return schedulerPersistence.NewPostgresPersistence(ctx, s.logger, persistenceURL)

	case "mysql":
		// Future: implement database persistence
		// return schedulerPersistence.NewMySQLPersistence(persistenceURL)
		return nil, errors.New("mysql persistence for scheduler not yet implemented")

	default:
		return nil, errors.New("unsupported scheduler persistence scheme: " + scheme + " (supported: file, postgres, mysql)")
	}
}

// parsePersistenceScheme extracts the scheme from a persistence URL.
func (s *SchedulerProvider) parsePersistenceScheme(persistenceURL string) string {
	parts := strings.Split(persistenceURL, "://")
	if len(parts) < 2 {
		return "file" // default to file if no scheme
	}

	return parts[0]
}
