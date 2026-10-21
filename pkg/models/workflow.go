// Package models defines the core domain models for node-based workflow automation
package models

import "time"

// WorkflowStatus represents the lifecycle state of a workflow.
type WorkflowStatus string

const (
	WorkflowStatusDraft       WorkflowStatus = "draft"       // Editable, not executable
	WorkflowStatusPublished   WorkflowStatus = "published"   // Current active, executable
	WorkflowStatusUnpublished WorkflowStatus = "unpublished" // Historical, not executable
)

// Workflow represents a node-based workflow with simplified versioning support.
type Workflow struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"                   validate:"required,min=3"`
	Description     string          `json:"description"            validate:"required"`
	Status          WorkflowStatus  `json:"status"                 validate:"required"`
	WorkflowGroupID string          `json:"workflow_group_id"      validate:"omitempty,uuid"` // Stable ID linking all versions
	Nodes           []*WorkflowNode `json:"nodes"`                                            // Node instances in the workflow
	Connections     []*Connection   `json:"connections"`                                      // Connections between nodes
	Variables       map[string]any  `json:"variables"`
	Metadata        map[string]any  `json:"metadata,omitempty"`
	Owner           string          `json:"owner"`
	TenantID        string          `json:"tenant_id"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	PublishedAt     *time.Time      `json:"published_at,omitempty"`
	DeletedAt       *time.Time      `json:"deleted_at,omitempty"`
}
