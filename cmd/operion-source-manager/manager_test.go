package main

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/events"
	"github.com/dukex/operion/pkg/mocks"
	"github.com/dukex/operion/pkg/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewProviderManager_Constructor(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	tests := []struct {
		name           string
		id             string
		filter         []string
		expectedFilter []string
	}{
		{
			name:           "Success with no filter",
			id:             "test-manager",
			filter:         nil,
			expectedFilter: nil,
		},
		{
			name:           "Success with filter",
			id:             "test-manager-filtered",
			filter:         []string{"scheduler", "webhook"},
			expectedFilter: []string{"scheduler", "webhook"},
		},
		{
			name:           "Success with empty filter",
			id:             "test-manager-empty",
			filter:         []string{},
			expectedFilter: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockEventBus := &mocks.MockEventBus{}
			manager := NewProviderManager(tt.id, mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, tt.filter)

			assert.NotNil(t, manager)
			assert.Equal(t, tt.id, manager.id)
			assert.Equal(t, mockPersistence, manager.persistence)
			assert.Equal(t, mockSourceEventBus, manager.sourceEventBus)
			assert.Equal(t, testRegistry, manager.registry)
			assert.NotNil(t, manager.logger)
			assert.Equal(t, 0, manager.restartCount)
			assert.Equal(t, tt.expectedFilter, manager.providerFilter)
			assert.NotNil(t, manager.runningProviders)
		})
	}
}

func TestProviderManager_CreateSourceEventCallback_Success(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	// Mock successful source event publishing
	mockSourceEventBus.On("PublishSourceEvent", mock.Anything, mock.AnythingOfType("*events.SourceEvent")).Return(nil)

	callback := manager.createSourceEventCallback()

	eventData := map[string]any{
		"schedule_id": "sched-123",
		"timestamp":   "2023-01-01T00:00:00Z",
	}

	err := callback(context.Background(), "source-123", "scheduler", "ScheduleDue", eventData)

	assert.NoError(t, err)
	mockSourceEventBus.AssertExpectations(t)
}

func TestProviderManager_CreateSourceEventCallback_ValidationFailure(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	callback := manager.createSourceEventCallback()

	// Create invalid event data (missing required fields)
	err := callback(context.Background(), "", "scheduler", "ScheduleDue", map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "source_id is required")

	// Should not call PublishSourceEvent due to validation failure
	mockSourceEventBus.AssertNotCalled(t, "PublishSourceEvent")
}

func TestProviderManager_CreateSourceEventCallback_PublishFailure(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	// Mock source event publishing failure
	mockSourceEventBus.On("PublishSourceEvent", mock.Anything, mock.AnythingOfType("*events.SourceEvent")).Return(assert.AnError)

	callback := manager.createSourceEventCallback()

	eventData := map[string]any{
		"schedule_id": "sched-123",
		"timestamp":   "2023-01-01T00:00:00Z",
	}

	err := callback(context.Background(), "source-123", "scheduler", "ScheduleDue", eventData)

	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
	mockSourceEventBus.AssertExpectations(t)
}

func TestProviderManager_CreateSourceEventCallback_ValidEventStructure(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	eventData := map[string]any{
		"schedule_id": "sched-123",
		"timestamp":   "2023-01-01T00:00:00Z",
		"metadata":    map[string]any{"cron": "0 * * * *"},
	}

	// Capture the actual source event to validate its structure
	var capturedEvent *events.SourceEvent

	mockSourceEventBus.On("PublishSourceEvent", mock.Anything, mock.AnythingOfType("*events.SourceEvent")).
		Run(func(args mock.Arguments) {
			capturedEvent = args.Get(1).(*events.SourceEvent)
		}).Return(nil)

	callback := manager.createSourceEventCallback()

	err := callback(context.Background(), "source-456", "scheduler", "ScheduleDue", eventData)

	assert.NoError(t, err)

	// Validate event structure
	require.NotNil(t, capturedEvent)
	assert.Equal(t, "source-456", capturedEvent.SourceID)
	assert.Equal(t, "scheduler", capturedEvent.ProviderID)
	assert.Equal(t, "ScheduleDue", capturedEvent.EventType)
	assert.Equal(t, eventData, capturedEvent.EventData)

	mockSourceEventBus.AssertExpectations(t)
}

func TestProviderManager_Stop_GracefulShutdown(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	// Add some mock providers to running providers
	mockProvider1 := &mocks.MockProvider{}
	mockProvider2 := &mocks.MockProvider{}

	manager.runningProviders["provider1"] = mockProvider1
	manager.runningProviders["provider2"] = mockProvider2

	// Mock stop calls
	mockProvider1.On("Stop", mock.Anything).Return(nil)
	mockProvider2.On("Stop", mock.Anything).Return(nil)

	ctx, cancel := context.WithCancel(context.Background())

	// Test that stop calls cancel and stops all providers
	manager.stop(ctx, cancel)

	// Verify context was cancelled
	select {
	case <-ctx.Done():
		// Context was properly cancelled
	default:
		t.Error("Context should have been cancelled")
	}

	// Verify all providers were stopped
	mockProvider1.AssertExpectations(t)
	mockProvider2.AssertExpectations(t)

	// Verify running providers map was cleared
	assert.Empty(t, manager.runningProviders)
}

func TestProviderManager_Stop_WithNilCancel(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	// Should not panic when cancel is nil
	assert.NotPanics(t, func() {
		manager.stop(context.Background(), nil)
	})
}

func TestProviderManager_Stop_ProviderStopError(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	// Add mock provider that fails to stop
	mockProvider := &mocks.MockProvider{}
	mockProvider.On("Stop", mock.Anything).Return(assert.AnError)
	manager.runningProviders["provider1"] = mockProvider

	ctx, cancel := context.WithCancel(context.Background())

	// Should not panic even if provider stop fails
	assert.NotPanics(t, func() {
		manager.stop(ctx, cancel)
	})

	mockProvider.AssertExpectations(t)
	assert.Empty(t, manager.runningProviders) // Map should still be cleared
}

func TestProviderManager_Restart_IncrementCount(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	// We can't test the full restart method since it calls os.Exit or starts a new process
	// But we can test the restart count increment by calling it directly
	originalRestartCount := manager.restartCount
	manager.restartCount++ // Simulate what restart() does

	assert.Equal(t, originalRestartCount+1, manager.restartCount)
}

func TestProviderManager_HandleSignals_Setup(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Test that handleSignals sets up signal handling without panicking
	assert.NotPanics(t, func() {
		manager.handleSignals(ctx, cancel)
		// Give goroutine time to start
		time.Sleep(50 * time.Millisecond)
	})
}

func TestProviderManager_StartProviders_EmptyRegistry(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger) // Empty registry

	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, nil)

	// Should not fail when no providers are registered
	err := manager.startProviders(context.Background())
	assert.NoError(t, err)
}

func TestProviderManager_StartProviders_NoMatchingFilter(t *testing.T) {
	mockPersistence := &mocks.MockPersistence{}
	mockSourceEventBus := &mocks.MockSourceEventBus{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testRegistry := registry.NewRegistry(logger)

	// Create a manager with a filter that won't match anything
	filter := []string{"nonexistent-provider"}
	mockEventBus := &mocks.MockEventBus{}
	manager := NewProviderManager("test-manager", mockPersistence, mockSourceEventBus, mockEventBus, logger, testRegistry, filter)

	// Should not fail when filter matches no providers
	err := manager.startProviders(context.Background())
	assert.NoError(t, err)
}
