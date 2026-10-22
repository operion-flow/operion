package models

import "time"

// ExecutionStatus represents the lifecycle state of a workflow execution.
type ExecutionStatus string

const (
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusCompleted ExecutionStatus = "completed"
	ExecutionStatusFailed    ExecutionStatus = "failed"
	ExecutionStatusCancelled ExecutionStatus = "cancelled"
	ExecutionStatusTimeout   ExecutionStatus = "timeout"
	ExecutionStatusPaused    ExecutionStatus = "paused"
)

// ExecutionContext represents the state of a node-based workflow execution.
type ExecutionContext struct {
	ID           string                `json:"id"`
	WorkflowID   string                `json:"workflow_id"             validate:"required"`
	Status       ExecutionStatus       `json:"status"`
	NodeResults  map[string]NodeResult `json:"node_results"`
	TriggerData  map[string]any        `json:"trigger_data,omitempty"`
	Variables    map[string]any        `json:"variables,omitempty"`
	Metadata     map[string]any        `json:"metadata,omitempty"`
	ErrorMessage string                `json:"error_message,omitempty"`
	CreatedAt    time.Time             `json:"created_at"`
	CompletedAt  *time.Time            `json:"completed_at,omitempty"`
	Secrets      map[string]string     `json:"-"`
}
