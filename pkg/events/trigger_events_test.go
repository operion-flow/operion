package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTriggerCreatedEvent_JSONSerialization(t *testing.T) {
	original := NewTriggerCreatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		map[string]any{"cron": "0 9 * * *"},
		"user-789",
	)

	// JSON serialization round-trip testing
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Contains(t, string(jsonData), `"trigger_id":"trigger-123"`)
	assert.Contains(t, string(jsonData), `"node_type":"trigger:scheduler"`)

	var deserialized TriggerCreatedEvent

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.TriggerID, deserialized.TriggerID)
	assert.Equal(t, original.WorkflowID, deserialized.WorkflowID)
	assert.Equal(t, original.NodeType, deserialized.NodeType)
	assert.Equal(t, original.Config, deserialized.Config)
	assert.Equal(t, original.CreatedBy, deserialized.CreatedBy)
	assert.Equal(t, TriggerCreatedEventType, deserialized.GetType())
}

func TestTriggerCreatedEvent_Validation(t *testing.T) {
	tests := []struct {
		name        string
		event       *TriggerCreatedEvent
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid_event",
			event: NewTriggerCreatedEvent(
				"trigger-123",
				"workflow-456",
				"trigger:scheduler",
				map[string]any{"cron": "0 9 * * *"},
				"user-789",
			),
			wantErr: false,
		},
		{
			name: "missing_trigger_id",
			event: &TriggerCreatedEvent{
				TriggerID:  "",
				WorkflowID: "workflow-456",
				NodeType:   "trigger:scheduler",
				Config:     map[string]any{"cron": "0 9 * * *"},
			},
			wantErr:     true,
			expectedErr: "trigger_id is required",
		},
		{
			name: "missing_workflow_id",
			event: &TriggerCreatedEvent{
				TriggerID:  "trigger-123",
				WorkflowID: "",
				NodeType:   "trigger:scheduler",
				Config:     map[string]any{"cron": "0 9 * * *"},
			},
			wantErr:     true,
			expectedErr: "workflow_id is required",
		},
		{
			name: "missing_node_type",
			event: &TriggerCreatedEvent{
				TriggerID:  "trigger-123",
				WorkflowID: "workflow-456",
				NodeType:   "",
				Config:     map[string]any{"cron": "0 9 * * *"},
			},
			wantErr:     true,
			expectedErr: "node_type is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTriggerUpdatedEvent_JSONSerialization(t *testing.T) {
	previousConfig := map[string]any{"cron": "0 8 * * *"}
	newConfig := map[string]any{"cron": "0 9 * * *"}

	original := NewTriggerUpdatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		newConfig,
		previousConfig,
		"user-789",
	)

	// JSON serialization round-trip testing
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)

	var deserialized TriggerUpdatedEvent

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.TriggerID, deserialized.TriggerID)
	assert.Equal(t, original.Config, deserialized.Config)
	assert.Equal(t, original.PreviousConfig, deserialized.PreviousConfig)
	assert.Equal(t, TriggerUpdatedEventType, deserialized.GetType())
}

func TestTriggerDeletedEvent_JSONSerialization(t *testing.T) {
	original := NewTriggerDeletedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		"source-789",
		"user-789",
	)

	// JSON serialization round-trip testing
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)

	var deserialized TriggerDeletedEvent

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.TriggerID, deserialized.TriggerID)
	assert.Equal(t, original.SourceID, deserialized.SourceID)
	assert.Equal(t, TriggerDeletedEventType, deserialized.GetType())
}

func TestWorkflowPublishedEvent_JSONSerialization(t *testing.T) {
	triggerNodes := []TriggerNode{
		{
			ID:     "trigger-1",
			Type:   "trigger:scheduler",
			Config: map[string]any{"cron": "0 9 * * *"},
		},
		{
			ID:     "trigger-2",
			Type:   "trigger:webhook",
			Config: map[string]any{"path": "/webhook"},
		},
	}

	original := NewWorkflowPublishedEvent(
		"workflow-123",
		"Test Workflow",
		triggerNodes,
		"user-789",
	)

	// JSON serialization round-trip testing
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)

	var deserialized WorkflowPublishedEvent

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.WorkflowID, deserialized.WorkflowID)
	assert.Equal(t, original.WorkflowName, deserialized.WorkflowName)
	assert.Len(t, deserialized.TriggerNodes, 2)
	assert.Equal(t, original.TriggerNodes[0].ID, deserialized.TriggerNodes[0].ID)
	assert.Equal(t, WorkflowPublishedEventType, deserialized.GetType())
}

func TestWorkflowPublishedEvent_Validation(t *testing.T) {
	tests := []struct {
		name        string
		event       *WorkflowPublishedEvent
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid_event",
			event: NewWorkflowPublishedEvent(
				"workflow-123",
				"Test Workflow",
				[]TriggerNode{{ID: "trigger-1", Type: "trigger:scheduler"}},
				"user-789",
			),
			wantErr: false,
		},
		{
			name: "missing_workflow_id",
			event: &WorkflowPublishedEvent{
				WorkflowID:   "",
				WorkflowName: "Test Workflow",
				TriggerNodes: []TriggerNode{},
			},
			wantErr:     true,
			expectedErr: "workflow_id is required",
		},
		{
			name: "missing_workflow_name",
			event: &WorkflowPublishedEvent{
				WorkflowID:   "workflow-123",
				WorkflowName: "",
				TriggerNodes: []TriggerNode{},
			},
			wantErr:     true,
			expectedErr: "workflow_name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEventCreationDefaults(t *testing.T) {
	event := NewTriggerCreatedEvent(
		"trigger-123",
		"workflow-456",
		"trigger:scheduler",
		nil, // nil config should be handled
		"user-789",
	)

	// Verify defaults are set correctly
	assert.NotEmpty(t, event.ID)
	assert.Equal(t, TriggerCreatedEventType, event.Type)
	assert.WithinDuration(t, time.Now(), event.Timestamp, 1*time.Second)
	assert.WithinDuration(t, time.Now(), event.CreatedAt, 1*time.Second)
	assert.NotNil(t, event.Config)
	assert.Equal(t, 0, len(event.Config))
}
