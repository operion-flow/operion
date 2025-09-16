// Package events defines trigger lifecycle domain events for event-driven trigger configuration.
package events

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Domain events for trigger lifecycle management.
const (
	TriggerCreatedEventType      EventType = "trigger.created"
	TriggerUpdatedEventType      EventType = "trigger.updated"
	TriggerDeletedEventType      EventType = "trigger.deleted"
	WorkflowPublishedEventType   EventType = "workflow.published"
	WorkflowUnpublishedEventType EventType = "workflow.unpublished"
)

// TriggerCreatedEvent is published when a new trigger node is created.
// This domain event announces that a trigger has been added to a workflow.
type TriggerCreatedEvent struct {
	ID         string         `json:"id"`
	Type       EventType      `json:"type"`
	Timestamp  time.Time      `json:"timestamp"`
	TriggerID  string         `json:"trigger_id"           validate:"required"`
	WorkflowID string         `json:"workflow_id"          validate:"required"`
	NodeType   string         `json:"node_type"            validate:"required"` // e.g., "trigger:scheduler"
	Config     map[string]any `json:"config"`                                   // Node configuration
	CreatedAt  time.Time      `json:"created_at"`
	CreatedBy  string         `json:"created_by,omitempty"`
}

// NewTriggerCreatedEvent creates a new TriggerCreatedEvent.
func NewTriggerCreatedEvent(triggerID, workflowID, nodeType string, config map[string]any, createdBy string) *TriggerCreatedEvent {
	if config == nil {
		config = make(map[string]any)
	}

	return &TriggerCreatedEvent{
		ID:         uuid.New().String(),
		Type:       TriggerCreatedEventType,
		Timestamp:  time.Now().UTC(),
		TriggerID:  triggerID,
		WorkflowID: workflowID,
		NodeType:   nodeType,
		Config:     config,
		CreatedAt:  time.Now().UTC(),
		CreatedBy:  createdBy,
	}
}

// GetType returns the event type for TriggerCreatedEvent.
func (t *TriggerCreatedEvent) GetType() EventType {
	return TriggerCreatedEventType
}

// Validate performs validation on the TriggerCreatedEvent.
func (t *TriggerCreatedEvent) Validate() error {
	if t.TriggerID == "" {
		return errors.New("trigger_id is required")
	}

	if t.WorkflowID == "" {
		return errors.New("workflow_id is required")
	}

	if t.NodeType == "" {
		return errors.New("node_type is required")
	}

	return nil
}

// TriggerUpdatedEvent is published when a trigger node configuration is updated.
type TriggerUpdatedEvent struct {
	ID             string         `json:"id"`
	Type           EventType      `json:"type"`
	Timestamp      time.Time      `json:"timestamp"`
	TriggerID      string         `json:"trigger_id"                validate:"required"`
	WorkflowID     string         `json:"workflow_id"               validate:"required"`
	NodeType       string         `json:"node_type"                 validate:"required"`
	Config         map[string]any `json:"config"`                    // New configuration
	PreviousConfig map[string]any `json:"previous_config,omitempty"` // Previous configuration for comparison
	UpdatedAt      time.Time      `json:"updated_at"`
	UpdatedBy      string         `json:"updated_by,omitempty"`
}

// NewTriggerUpdatedEvent creates a new TriggerUpdatedEvent.
func NewTriggerUpdatedEvent(triggerID, workflowID, nodeType string, config, previousConfig map[string]any, updatedBy string) *TriggerUpdatedEvent {
	if config == nil {
		config = make(map[string]any)
	}

	return &TriggerUpdatedEvent{
		ID:             uuid.New().String(),
		Type:           TriggerUpdatedEventType,
		Timestamp:      time.Now().UTC(),
		TriggerID:      triggerID,
		WorkflowID:     workflowID,
		NodeType:       nodeType,
		Config:         config,
		PreviousConfig: previousConfig,
		UpdatedAt:      time.Now().UTC(),
		UpdatedBy:      updatedBy,
	}
}

// GetType returns the event type for TriggerUpdatedEvent.
func (t *TriggerUpdatedEvent) GetType() EventType {
	return TriggerUpdatedEventType
}

// Validate performs validation on the TriggerUpdatedEvent.
func (t *TriggerUpdatedEvent) Validate() error {
	if t.TriggerID == "" {
		return errors.New("trigger_id is required")
	}

	if t.WorkflowID == "" {
		return errors.New("workflow_id is required")
	}

	if t.NodeType == "" {
		return errors.New("node_type is required")
	}

	return nil
}

// TriggerDeletedEvent is published when a trigger node is deleted.
type TriggerDeletedEvent struct {
	ID         string    `json:"id"`
	Type       EventType `json:"type"`
	Timestamp  time.Time `json:"timestamp"`
	TriggerID  string    `json:"trigger_id"           validate:"required"`
	WorkflowID string    `json:"workflow_id"          validate:"required"`
	NodeType   string    `json:"node_type"            validate:"required"`
	SourceID   string    `json:"source_id,omitempty"` // Source ID if it was configured
	DeletedAt  time.Time `json:"deleted_at"`
	DeletedBy  string    `json:"deleted_by,omitempty"`
}

// NewTriggerDeletedEvent creates a new TriggerDeletedEvent.
func NewTriggerDeletedEvent(triggerID, workflowID, nodeType, sourceID, deletedBy string) *TriggerDeletedEvent {
	return &TriggerDeletedEvent{
		ID:         uuid.New().String(),
		Type:       TriggerDeletedEventType,
		Timestamp:  time.Now().UTC(),
		TriggerID:  triggerID,
		WorkflowID: workflowID,
		NodeType:   nodeType,
		SourceID:   sourceID,
		DeletedAt:  time.Now().UTC(),
		DeletedBy:  deletedBy,
	}
}

// GetType returns the event type for TriggerDeletedEvent.
func (t *TriggerDeletedEvent) GetType() EventType {
	return TriggerDeletedEventType
}

// Validate performs validation on the TriggerDeletedEvent.
func (t *TriggerDeletedEvent) Validate() error {
	if t.TriggerID == "" {
		return errors.New("trigger_id is required")
	}

	if t.WorkflowID == "" {
		return errors.New("workflow_id is required")
	}

	if t.NodeType == "" {
		return errors.New("node_type is required")
	}

	return nil
}

// TriggerNode represents a trigger node in workflow published events.
type TriggerNode struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Config   map[string]any `json:"config"`
	SourceID string         `json:"source_id,omitempty"`
}

// WorkflowPublishedEvent is published when a workflow with triggers is published.
// This event contains all trigger nodes that need source configuration.
type WorkflowPublishedEvent struct {
	ID           string        `json:"id"`
	Type         EventType     `json:"type"`
	Timestamp    time.Time     `json:"timestamp"`
	WorkflowID   string        `json:"workflow_id"            validate:"required"`
	WorkflowName string        `json:"workflow_name"          validate:"required"`
	TriggerNodes []TriggerNode `json:"trigger_nodes"` // All trigger nodes in the workflow
	PublishedAt  time.Time     `json:"published_at"`
	PublishedBy  string        `json:"published_by,omitempty"`
}

// NewWorkflowPublishedEvent creates a new WorkflowPublishedEvent.
func NewWorkflowPublishedEvent(workflowID, workflowName string, triggerNodes []TriggerNode, publishedBy string) *WorkflowPublishedEvent {
	if triggerNodes == nil {
		triggerNodes = make([]TriggerNode, 0)
	}

	return &WorkflowPublishedEvent{
		ID:           uuid.New().String(),
		Type:         WorkflowPublishedEventType,
		Timestamp:    time.Now().UTC(),
		WorkflowID:   workflowID,
		WorkflowName: workflowName,
		TriggerNodes: triggerNodes,
		PublishedAt:  time.Now().UTC(),
		PublishedBy:  publishedBy,
	}
}

// GetType returns the event type for WorkflowPublishedEvent.
func (w *WorkflowPublishedEvent) GetType() EventType {
	return WorkflowPublishedEventType
}

// Validate performs validation on the WorkflowPublishedEvent.
func (w *WorkflowPublishedEvent) Validate() error {
	if w.WorkflowID == "" {
		return errors.New("workflow_id is required")
	}

	if w.WorkflowName == "" {
		return errors.New("workflow_name is required")
	}

	return nil
}

// WorkflowUnpublishedEvent is published when a published workflow is unpublished.
// This event signals that all sources for the workflow's triggers should be removed.
type WorkflowUnpublishedEvent struct {
	ID            string        `json:"id"`
	Type          EventType     `json:"type"`
	Timestamp     time.Time     `json:"timestamp"`
	WorkflowID    string        `json:"workflow_id"              validate:"required"`
	WorkflowName  string        `json:"workflow_name"            validate:"required"`
	TriggerNodes  []TriggerNode `json:"trigger_nodes"` // All trigger nodes that were active
	UnpublishedAt time.Time     `json:"unpublished_at"`
	UnpublishedBy string        `json:"unpublished_by,omitempty"`
}

// NewWorkflowUnpublishedEvent creates a new WorkflowUnpublishedEvent.
func NewWorkflowUnpublishedEvent(workflowID, workflowName string, triggerNodes []TriggerNode, unpublishedBy string) *WorkflowUnpublishedEvent {
	if triggerNodes == nil {
		triggerNodes = make([]TriggerNode, 0)
	}

	return &WorkflowUnpublishedEvent{
		ID:            uuid.New().String(),
		Type:          WorkflowUnpublishedEventType,
		Timestamp:     time.Now().UTC(),
		WorkflowID:    workflowID,
		WorkflowName:  workflowName,
		TriggerNodes:  triggerNodes,
		UnpublishedAt: time.Now().UTC(),
		UnpublishedBy: unpublishedBy,
	}
}

// GetType returns the event type for WorkflowUnpublishedEvent.
func (w *WorkflowUnpublishedEvent) GetType() EventType {
	return WorkflowUnpublishedEventType
}

// Validate performs validation on the WorkflowUnpublishedEvent.
func (w *WorkflowUnpublishedEvent) Validate() error {
	if w.WorkflowID == "" {
		return errors.New("workflow_id is required")
	}

	if w.WorkflowName == "" {
		return errors.New("workflow_name is required")
	}

	return nil
}
