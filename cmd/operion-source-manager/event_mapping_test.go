package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukex/operion/pkg/events"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
)

func TestProviderManager_extractProviderID(t *testing.T) {
	spm := &ProviderManager{
		logger: slog.Default(), // Add logger to prevent nil pointer dereference
	} // Minimal manager for testing

	tests := []struct {
		name     string
		nodeType string
		expected string
	}{
		{
			name:     "scheduler_trigger",
			nodeType: "trigger:scheduler",
			expected: "scheduler",
		},
		{
			name:     "webhook_trigger",
			nodeType: "trigger:webhook",
			expected: "webhook",
		},
		{
			name:     "kafka_trigger",
			nodeType: "trigger:kafka",
			expected: "kafka",
		},
		{
			name:     "invalid_format",
			nodeType: "invalid",
			expected: "invalid",
		},
		{
			name:     "empty_string",
			nodeType: "",
			expected: "",
		},
		{
			name:     "action_node",
			nodeType: "action:transform",
			expected: "action:transform", // Should fallback to full nodeType for non-trigger nodes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := spm.extractProviderID(tt.nodeType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestProviderManager_eventToTriggerConfig(t *testing.T) {
	spm := &ProviderManager{}

	config := map[string]any{
		"cron_expression": "0 9 * * *",
		"timezone":        "UTC",
	}

	event := events.NewTriggerCreatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		config,
		"user-789",
	)

	triggerConfig := spm.eventToTriggerConfig(event)

	// Verify transformation
	assert.Equal(t, event.TriggerID, triggerConfig.TriggerID)
	assert.Equal(t, event.WorkflowID, triggerConfig.WorkflowID)
	assert.Equal(t, event.NodeType, triggerConfig.NodeType)
	assert.Equal(t, event.Config, triggerConfig.Config)
	assert.Equal(t, "scheduler", triggerConfig.ProviderID)
}

func TestProviderManager_updatedEventToTriggerConfig(t *testing.T) {
	spm := &ProviderManager{}

	previousConfig := map[string]any{"cron_expression": "0 8 * * *"}
	newConfig := map[string]any{"cron_expression": "0 9 * * *"}

	event := events.NewTriggerUpdatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		newConfig,
		previousConfig,
		"user-789",
	)

	triggerConfig := spm.updatedEventToTriggerConfig(event)

	// Verify transformation uses new config, not previous
	assert.Equal(t, event.TriggerID, triggerConfig.TriggerID)
	assert.Equal(t, event.WorkflowID, triggerConfig.WorkflowID)
	assert.Equal(t, event.NodeType, triggerConfig.NodeType)
	assert.Equal(t, newConfig, triggerConfig.Config)
	assert.Equal(t, "scheduler", triggerConfig.ProviderID)
}

func TestProviderManager_nodeToTriggerConfig(t *testing.T) {
	spm := &ProviderManager{}

	node := events.TriggerNode{
		ID:     "trigger-123",
		Type:   "trigger:webhook",
		Config: map[string]any{"path": "/webhook"},
	}

	workflowID := "workflow-456"

	triggerConfig := spm.nodeToTriggerConfig(workflowID, node)

	// Verify transformation
	assert.Equal(t, node.ID, triggerConfig.TriggerID)
	assert.Equal(t, workflowID, triggerConfig.WorkflowID)
	assert.Equal(t, node.Type, triggerConfig.NodeType)
	assert.Equal(t, node.Config, triggerConfig.Config)
	assert.Equal(t, "webhook", triggerConfig.ProviderID)
}

func TestProviderManager_workflowNodeToTriggerConfig(t *testing.T) {
	spm := &ProviderManager{}

	node := &models.WorkflowNode{
		ID:     "trigger-123",
		Type:   "trigger:kafka",
		Config: map[string]any{"topic": "test-topic"},
	}

	workflowID := "workflow-456"

	triggerConfig := spm.workflowNodeToTriggerConfig(workflowID, node)

	// Verify transformation
	assert.Equal(t, node.ID, triggerConfig.TriggerID)
	assert.Equal(t, workflowID, triggerConfig.WorkflowID)
	assert.Equal(t, node.Type, triggerConfig.NodeType)
	assert.Equal(t, node.Config, triggerConfig.Config)
	assert.Equal(t, "kafka", triggerConfig.ProviderID)
}

func TestProviderManager_getTriggerConfigValidationErrors(t *testing.T) {
	spm := &ProviderManager{}

	tests := []struct {
		name        string
		config      protocol.TriggerConfig
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid_config",
			config: protocol.TriggerConfig{
				TriggerID:  "trigger-123",
				WorkflowID: "workflow-456",
				NodeType:   "trigger:scheduler",
				ProviderID: "scheduler",
				Config:     map[string]any{"cron": "0 9 * * *"},
			},
			wantErr: false,
		},
		{
			name: "missing_trigger_id",
			config: protocol.TriggerConfig{
				TriggerID:  "",
				WorkflowID: "workflow-456",
				NodeType:   "trigger:scheduler",
				ProviderID: "scheduler",
			},
			wantErr:     true,
			expectedErr: "trigger_id is required",
		},
		{
			name: "missing_workflow_id",
			config: protocol.TriggerConfig{
				TriggerID:  "trigger-123",
				WorkflowID: "",
				NodeType:   "trigger:scheduler",
				ProviderID: "scheduler",
			},
			wantErr:     true,
			expectedErr: "workflow_id is required",
		},
		{
			name: "missing_node_type",
			config: protocol.TriggerConfig{
				TriggerID:  "trigger-123",
				WorkflowID: "workflow-456",
				NodeType:   "",
				ProviderID: "scheduler",
			},
			wantErr:     true,
			expectedErr: "node_type is required",
		},
		{
			name: "missing_provider_id",
			config: protocol.TriggerConfig{
				TriggerID:  "trigger-123",
				WorkflowID: "workflow-456",
				NodeType:   "trigger:scheduler",
				ProviderID: "",
			},
			wantErr:     true,
			expectedErr: "provider_id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := spm.getTriggerConfigValidationErrors(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)

				// Verify it's a ConfigurationError
				require.True(t, IsConfigurationError(err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigurationError(t *testing.T) {
	err := &ConfigurationError{
		Type:    "validation_error",
		Message: "test error message",
		Details: map[string]any{"field": "trigger_id"},
	}

	// Test error interface
	assert.Equal(t, "test error message", err.Error())

	// Test type checking
	assert.True(t, IsConfigurationError(err))
	assert.False(t, IsConfigurationError(assert.AnError))
}

func TestProviderManager_shouldConfigureSource(t *testing.T) {
	spm := &ProviderManager{
		runningProviders: map[string]protocol.Provider{
			"scheduler": nil, // Just need the key to exist for isProviderRunning
			"webhook":   nil,
		},
	}

	tests := []struct {
		name           string
		triggerConfig  protocol.TriggerConfig
		expectedResult bool
	}{
		{
			name: "running_provider",
			triggerConfig: protocol.TriggerConfig{
				ProviderID: "scheduler",
			},
			expectedResult: true,
		},
		{
			name: "non_running_provider",
			triggerConfig: protocol.TriggerConfig{
				ProviderID: "kafka",
			},
			expectedResult: false,
		},
		{
			name: "empty_provider",
			triggerConfig: protocol.TriggerConfig{
				ProviderID: "",
			},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := spm.shouldConfigureSource(tt.triggerConfig)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestProviderManager_isProviderRunning(t *testing.T) {
	spm := &ProviderManager{
		runningProviders: map[string]protocol.Provider{
			"scheduler": nil,
			"webhook":   nil,
		},
	}

	// Test existing provider
	assert.True(t, spm.isProviderRunning("scheduler"))
	assert.True(t, spm.isProviderRunning("webhook"))

	// Test non-existing provider
	assert.False(t, spm.isProviderRunning("kafka"))
	assert.False(t, spm.isProviderRunning("nonexistent"))
	assert.False(t, spm.isProviderRunning(""))
}
