package main

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dukex/operion/pkg/events"
	"github.com/dukex/operion/pkg/mocks"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/protocol"
)

// Mock provider for testing - implements both Provider and ProviderLifecycle interfaces.
type MockProvider struct {
	mock.Mock
}

// Provider interface methods.
func (m *MockProvider) Start(ctx context.Context, callback protocol.SourceEventCallback) error {
	args := m.Called(ctx, callback)

	return args.Error(0)
}

func (m *MockProvider) Stop(ctx context.Context) error {
	args := m.Called(ctx)

	return args.Error(0)
}

func (m *MockProvider) Validate() error {
	args := m.Called()

	return args.Error(0)
}

// ProviderLifecycle interface methods.
func (m *MockProvider) Initialize(ctx context.Context, deps protocol.Dependencies) error {
	args := m.Called(ctx, deps)

	return args.Error(0)
}

func (m *MockProvider) Configure(workflows []*models.Workflow) (map[string]string, error) {
	args := m.Called(workflows)

	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *MockProvider) ConfigureTrigger(ctx context.Context, trigger protocol.TriggerConfig) (string, error) {
	args := m.Called(ctx, trigger)

	return args.String(0), args.Error(1)
}

func (m *MockProvider) RemoveTrigger(ctx context.Context, triggerID, sourceID string) error {
	args := m.Called(ctx, triggerID, sourceID)

	return args.Error(0)
}

func (m *MockProvider) Prepare(ctx context.Context) error {
	args := m.Called(ctx)

	return args.Error(0)
}

func TestProviderManager_handleTriggerCreatedEvent(t *testing.T) {
	// Create mock persistence and workflow repository
	mockPersistence := mocks.NewMockPersistence()
	mockWorkflowRepo := mockPersistence.GetMockWorkflowRepository()

	mockWorkflowRepo.On("ListWorkflows", mock.Anything, mock.Anything).Return(
		&persistence.WorkflowListResult{Workflows: []*models.Workflow{}}, nil)
	mockWorkflowRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	// Create a test provider manager
	spm := &ProviderManager{
		runningProviders: make(map[string]protocol.Provider),
		providerMutex:    sync.RWMutex{},
		logger:           slog.Default(),
		persistence:      mockPersistence,
	}

	// Create and register mock provider
	mockProvider := new(MockProvider)
	spm.runningProviders["scheduler"] = mockProvider

	// Test event
	event := events.NewTriggerCreatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		map[string]any{"cron_expression": "0 9 * * *"},
		"user-789",
	)

	// Setup mock expectations
	expectedConfig := protocol.TriggerConfig{
		TriggerID:  "trigger-123",
		WorkflowID: "workflow-456",
		NodeType:   "trigger:scheduler",
		Config:     event.Config,
		ProviderID: "scheduler",
	}

	mockProvider.On("ConfigureTrigger", mock.Anything, expectedConfig).Return("source-abc", nil)

	// Execute handler
	ctx := context.Background()
	err := spm.handleTriggerCreatedEvent(ctx, event)

	// Verify results
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestProviderManager_handleTriggerCreatedEvent_InvalidEventType(t *testing.T) {
	spm := &ProviderManager{
		logger: slog.Default(),
	}

	// Pass invalid event type
	err := spm.handleTriggerCreatedEvent(context.Background(), "invalid-event-type")

	// Should return error for invalid event type
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid event type for trigger.created")
}

func TestProviderManager_handleTriggerCreatedEvent_ProviderNotRunning(t *testing.T) {
	spm := &ProviderManager{
		runningProviders: make(map[string]protocol.Provider),
		providerMutex:    sync.RWMutex{},
		logger:           slog.Default(),
	}

	// No providers registered

	event := events.NewTriggerCreatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		map[string]any{"cron_expression": "0 9 * * *"},
		"user-789",
	)

	// Execute handler
	ctx := context.Background()
	err := spm.handleTriggerCreatedEvent(ctx, event)

	// Should succeed but not configure (shouldConfigureSource returns false)
	assert.NoError(t, err)
}

func TestProviderManager_handleTriggerUpdatedEvent(t *testing.T) {
	// Create mock persistence and workflow repository
	mockPersistence := mocks.NewMockPersistence()
	mockWorkflowRepo := mockPersistence.GetMockWorkflowRepository()

	mockWorkflowRepo.On("ListWorkflows", mock.Anything, mock.Anything).Return(
		&persistence.WorkflowListResult{Workflows: []*models.Workflow{}}, nil)
	mockWorkflowRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	spm := &ProviderManager{
		runningProviders: make(map[string]protocol.Provider),
		providerMutex:    sync.RWMutex{},
		logger:           slog.Default(),
		persistence:      mockPersistence,
	}

	// Create and register mock provider
	mockProvider := new(MockProvider)
	spm.runningProviders["webhook"] = mockProvider

	// Test event
	event := events.NewTriggerUpdatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:webhook",
		map[string]any{"path": "/new-webhook"},
		map[string]any{"path": "/old-webhook"},
		"user-789",
	)

	// Setup mock expectations - reconfigure calls the same ConfigureTrigger
	expectedConfig := protocol.TriggerConfig{
		TriggerID:  "trigger-123",
		WorkflowID: "workflow-456",
		NodeType:   "trigger:webhook",
		Config:     event.Config,
		ProviderID: "webhook",
	}

	mockProvider.On("ConfigureTrigger", mock.Anything, expectedConfig).Return("source-def", nil)

	// Execute handler
	ctx := context.Background()
	err := spm.handleTriggerUpdatedEvent(ctx, event)

	// Verify results
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestProviderManager_handleTriggerDeletedEvent(t *testing.T) {
	// Create mock persistence and workflow repository
	mockPersistence := mocks.NewMockPersistence()
	mockWorkflowRepo := mockPersistence.GetMockWorkflowRepository()

	mockWorkflowRepo.On("ListWorkflows", mock.Anything, mock.Anything).Return(
		&persistence.WorkflowListResult{Workflows: []*models.Workflow{}}, nil)
	mockWorkflowRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	spm := &ProviderManager{
		runningProviders: make(map[string]protocol.Provider),
		providerMutex:    sync.RWMutex{},
		logger:           slog.Default(),
		persistence:      mockPersistence,
	}

	// Create and register mock provider
	mockProvider := new(MockProvider)
	spm.runningProviders["scheduler"] = mockProvider

	// Test event with source ID
	event := events.NewTriggerDeletedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		"source-abc",
		"user-789",
	)

	// Setup mock expectations
	mockProvider.On("RemoveTrigger", mock.Anything, "trigger-123", "source-abc").Return(nil)

	// Execute handler
	ctx := context.Background()
	err := spm.handleTriggerDeletedEvent(ctx, event)

	// Verify results
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestProviderManager_handleTriggerDeletedEvent_NoSourceID(t *testing.T) {
	spm := &ProviderManager{
		logger: slog.Default(),
	}

	// Test event without source ID
	event := events.NewTriggerDeletedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		"", // No source ID
		"user-789",
	)

	// Execute handler
	ctx := context.Background()
	err := spm.handleTriggerDeletedEvent(ctx, event)

	// Should succeed without calling provider (no source to remove)
	assert.NoError(t, err)
}

func TestProviderManager_handleWorkflowPublishedEvent(t *testing.T) {
	// Create mock persistence and workflow repository
	mockPersistence := mocks.NewMockPersistence()
	mockWorkflowRepo := mockPersistence.GetMockWorkflowRepository()

	mockWorkflowRepo.On("ListWorkflows", mock.Anything, mock.Anything).Return(
		&persistence.WorkflowListResult{Workflows: []*models.Workflow{}}, nil)
	mockWorkflowRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	spm := &ProviderManager{
		runningProviders: make(map[string]protocol.Provider),
		providerMutex:    sync.RWMutex{},
		logger:           slog.Default(),
		persistence:      mockPersistence,
	}

	// Create and register mock providers
	mockSchedulerProvider := new(MockProvider)
	mockWebhookProvider := new(MockProvider)
	spm.runningProviders["scheduler"] = mockSchedulerProvider
	spm.runningProviders["webhook"] = mockWebhookProvider

	// Test event with multiple trigger nodes
	triggerNodes := []events.TriggerNode{
		{
			ID:     "trigger-1",
			Type:   "trigger:scheduler",
			Config: map[string]any{"cron_expression": "0 9 * * *"},
		},
		{
			ID:     "trigger-2",
			Type:   "trigger:webhook",
			Config: map[string]any{"path": "/webhook"},
		},
	}

	event := events.NewWorkflowPublishedEvent(
		"workflow-123",
		"Test Workflow",
		triggerNodes,
		"user-789",
	)

	// Setup mock expectations
	schedulerConfig := protocol.TriggerConfig{
		TriggerID:  "trigger-1",
		WorkflowID: "workflow-123",
		NodeType:   "trigger:scheduler",
		Config:     triggerNodes[0].Config,
		ProviderID: "scheduler",
	}

	webhookConfig := protocol.TriggerConfig{
		TriggerID:  "trigger-2",
		WorkflowID: "workflow-123",
		NodeType:   "trigger:webhook",
		Config:     triggerNodes[1].Config,
		ProviderID: "webhook",
	}

	mockSchedulerProvider.On("ConfigureTrigger", mock.Anything, schedulerConfig).Return("source-1", nil)
	mockWebhookProvider.On("ConfigureTrigger", mock.Anything, webhookConfig).Return("source-2", nil)

	// Execute handler
	ctx := context.Background()
	err := spm.handleWorkflowPublishedEvent(ctx, event)

	// Verify results
	assert.NoError(t, err)
	mockSchedulerProvider.AssertExpectations(t)
	mockWebhookProvider.AssertExpectations(t)
}

func TestProviderManager_handleWorkflowUnpublishedEvent(t *testing.T) {
	// Create mock persistence and workflow repository
	mockPersistence := mocks.NewMockPersistence()
	mockWorkflowRepo := mockPersistence.GetMockWorkflowRepository()

	mockWorkflowRepo.On("ListWorkflows", mock.Anything, mock.Anything).Return(
		&persistence.WorkflowListResult{Workflows: []*models.Workflow{}}, nil)
	mockWorkflowRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	spm := &ProviderManager{
		runningProviders: make(map[string]protocol.Provider),
		providerMutex:    sync.RWMutex{},
		logger:           slog.Default(),
		persistence:      mockPersistence,
	}

	// Create and register mock provider
	mockProvider := new(MockProvider)
	spm.runningProviders["scheduler"] = mockProvider

	// Test event with trigger nodes that have source IDs
	triggerNodes := []events.TriggerNode{
		{
			ID:       "trigger-1",
			Type:     "trigger:scheduler",
			Config:   map[string]any{"cron_expression": "0 9 * * *"},
			SourceID: "source-1",
		},
		{
			ID:       "trigger-2",
			Type:     "trigger:scheduler",
			Config:   map[string]any{"cron_expression": "0 10 * * *"},
			SourceID: "", // No source ID - should be skipped
		},
	}

	event := events.NewWorkflowUnpublishedEvent(
		"workflow-123",
		"Test Workflow",
		triggerNodes,
		"user-789",
	)

	// Setup mock expectations - only one call for the trigger with source ID
	mockProvider.On("RemoveTrigger", mock.Anything, "trigger-1", "source-1").Return(nil)

	// Execute handler
	ctx := context.Background()
	err := spm.handleWorkflowUnpublishedEvent(ctx, event)

	// Verify results
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestProviderManager_handleWorkflowPublishedEvent_InvalidEventType(t *testing.T) {
	spm := &ProviderManager{
		logger: slog.Default(),
	}

	// Pass invalid event type
	err := spm.handleWorkflowPublishedEvent(context.Background(), "invalid-event-type")

	// Should return error for invalid event type
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid event type for workflow.published")
}

func TestProviderManager_handleWorkflowPublishedEvent_ValidationError(t *testing.T) {
	spm := &ProviderManager{
		logger: slog.Default(),
	}

	// Test event with invalid trigger node (missing trigger ID)
	triggerNodes := []events.TriggerNode{
		{
			ID:     "", // Invalid - empty ID
			Type:   "trigger:scheduler",
			Config: map[string]any{"cron_expression": "0 9 * * *"},
		},
	}

	event := events.NewWorkflowPublishedEvent(
		"workflow-123",
		"Test Workflow",
		triggerNodes,
		"user-789",
	)

	// Execute handler
	ctx := context.Background()
	err := spm.handleWorkflowPublishedEvent(ctx, event)

	// Should succeed but log errors for invalid triggers
	assert.NoError(t, err)
}

// Integration test for the full event handling chain.
func TestProviderManager_EventHandling_Integration(t *testing.T) {
	// Create mock persistence and workflow repository
	mockPersistence := mocks.NewMockPersistence()
	mockWorkflowRepo := mockPersistence.GetMockWorkflowRepository()

	mockWorkflowRepo.On("ListWorkflows", mock.Anything, mock.Anything).Return(
		&persistence.WorkflowListResult{Workflows: []*models.Workflow{}}, nil)
	mockWorkflowRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	spm := &ProviderManager{
		runningProviders: make(map[string]protocol.Provider),
		providerMutex:    sync.RWMutex{},
		logger:           slog.Default(),
		persistence:      mockPersistence,
	}

	// Register mock provider
	mockProvider := new(MockProvider)
	spm.runningProviders["scheduler"] = mockProvider

	ctx := context.Background()

	// Test complete flow: create -> update -> delete

	// 1. Create trigger
	createEvent := events.NewTriggerCreatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		map[string]any{"cron_expression": "0 9 * * *"},
		"user-789",
	)

	createConfig := protocol.TriggerConfig{
		TriggerID:  "trigger-123",
		WorkflowID: "workflow-456",
		NodeType:   "trigger:scheduler",
		Config:     createEvent.Config,
		ProviderID: "scheduler",
	}

	mockProvider.On("ConfigureTrigger", mock.Anything, createConfig).Return("source-abc", nil).Once()

	err := spm.handleTriggerCreatedEvent(ctx, createEvent)
	require.NoError(t, err)

	// 2. Update trigger
	updateEvent := events.NewTriggerUpdatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		map[string]any{"cron_expression": "0 10 * * *"},
		map[string]any{"cron_expression": "0 9 * * *"},
		"user-789",
	)

	updateConfig := protocol.TriggerConfig{
		TriggerID:  "trigger-123",
		WorkflowID: "workflow-456",
		NodeType:   "trigger:scheduler",
		Config:     updateEvent.Config,
		ProviderID: "scheduler",
	}

	mockProvider.On("ConfigureTrigger", mock.Anything, updateConfig).Return("source-abc", nil).Once()

	err = spm.handleTriggerUpdatedEvent(ctx, updateEvent)
	require.NoError(t, err)

	// 3. Delete trigger
	deleteEvent := events.NewTriggerDeletedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		"source-abc",
		"user-789",
	)

	mockProvider.On("RemoveTrigger", mock.Anything, "trigger-123", "source-abc").Return(nil).Once()

	err = spm.handleTriggerDeletedEvent(ctx, deleteEvent)
	require.NoError(t, err)

	// Verify all expectations
	mockProvider.AssertExpectations(t)
}
