// Package models defines core node-based workflow models for graph execution
package models

import (
	"time"
)

// CategoryType represents the category of node.
type CategoryType string

const (
	CategoryTypeAction  CategoryType = "action"  // Regular action nodes (http, log, transform, etc.)
	CategoryTypeTrigger CategoryType = "trigger" // Trigger nodes (webhook, scheduler, kafka, etc.)
)

// Built-in trigger node types.
const (
	NodeTypeTriggerWebhook   = "trigger:webhook"
	NodeTypeTriggerScheduler = "trigger:scheduler"
	NodeTypeTriggerKafka     = "trigger:kafka"
)

// Connection connects two ports directly (fully normalized).
type Connection struct {
	ID         string `json:"id"`
	SourcePort string `json:"source_port" validate:"required"` // References Port.ID: "{node_id}:{port_name}"
	TargetPort string `json:"target_port" validate:"required"` // References Port.ID: "{node_id}:{port_name}"
}

// WorkflowNode represents a node instance in a workflow.
type WorkflowNode struct {
	ID         string         `json:"id"                    validate:"required"`
	Type       string         `json:"type"                  validate:"required"`
	Category   CategoryType   `json:"category"              validate:"required"`
	Config     map[string]any `json:"config"`
	PositionX  int            `json:"position_x"`
	PositionY  int            `json:"position_y"`
	Name       string         `json:"name"                  validate:"required,min=1"`
	Enabled    bool           `json:"enabled"`
	SourceID   *string        `json:"source_id,omitempty"`   // For trigger nodes only
	ProviderID *string        `json:"provider_id,omitempty"` // For trigger nodes only
	EventType  *string        `json:"event_type,omitempty"`  // For trigger nodes only
}

// Helper methods for category checking.
func (n *WorkflowNode) IsActionNode() bool {
	return n.Category == CategoryTypeAction
}

func (n *WorkflowNode) IsTriggerNode() bool {
	return n.Category == CategoryTypeTrigger
}

// NodeResult represents the result of a node execution.
type NodeResult struct {
	NodeID    string         `json:"node_id"`
	Data      map[string]any `json:"data"`
	Status    string         `json:"status"`
	Timestamp time.Time      `json:"timestamp"`
	Error     string         `json:"error,omitempty"`
}

// NodeStatus defines the possible states of a node execution.
type NodeStatus string

const (
	NodeStatusPending NodeStatus = "pending"
	NodeStatusRunning NodeStatus = "running"
	NodeStatusSuccess NodeStatus = "success"
	NodeStatusError   NodeStatus = "error"
	NodeStatusWaiting NodeStatus = "waiting"
)
