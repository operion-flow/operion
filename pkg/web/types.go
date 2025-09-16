// Package web provides HTTP request and response types for the workflow API.
package web

import "github.com/dukex/operion/pkg/models"

// ErrorResponse represents a standardized API error response.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// CreateWorkflowRequest represents the request body for creating a new workflow.
type CreateWorkflowRequest struct {
	Name        string         `json:"name"               validate:"required,min=3"`
	Description string         `json:"description"        validate:"required"`
	Variables   map[string]any `json:"variables"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Owner       string         `json:"owner"              validate:"required"`
}

// UpdateWorkflowRequest represents the request body for updating an existing workflow.
// All fields are optional to support partial updates.
type UpdateWorkflowRequest struct {
	Name        *string        `json:"name,omitempty"        validate:"omitempty,min=3"`
	Description *string        `json:"description,omitempty"`
	Variables   map[string]any `json:"variables,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// CreateNodeRequest represents the request body for creating a new workflow node.
type CreateNodeRequest struct {
	Type      string         `json:"type"       validate:"required"`
	Category  string         `json:"category"   validate:"required,oneof=action trigger"`
	Config    map[string]any `json:"config"`
	PositionX int            `json:"position_x"`
	PositionY int            `json:"position_y"`
	Name      string         `json:"name"       validate:"required,min=1"`
	Enabled   bool           `json:"enabled"`
}

// UpdateNodeRequest represents the request body for updating an existing workflow node.
// Type and Category cannot be changed, only config, position, name, and enabled status.
type UpdateNodeRequest struct {
	Config    map[string]any `json:"config"`
	PositionX int            `json:"position_x"`
	PositionY int            `json:"position_y"`
	Name      string         `json:"name"       validate:"required,min=1"`
	Enabled   bool           `json:"enabled"`
}

// NodeResponse represents the filtered response for a node.
type NodeResponse struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Category   string         `json:"category"`
	Name       string         `json:"name"`
	Config     map[string]any `json:"config"`
	Enabled    bool           `json:"enabled"`
	PositionX  int            `json:"position_x"`
	PositionY  int            `json:"position_y"`
	ProviderID *string        `json:"provider_id,omitempty"`
	EventType  *string        `json:"event_type,omitempty"`
}

// TransformNodeResponse transforms a WorkflowNode into a NodeResponse with appropriate filtering.
func TransformNodeResponse(node *models.WorkflowNode) NodeResponse {
	response := NodeResponse{
		ID:        node.ID,
		Type:      node.Type,
		Category:  string(node.Category),
		Name:      node.Name,
		Config:    node.Config,
		Enabled:   node.Enabled,
		PositionX: node.PositionX,
		PositionY: node.PositionY,
	}

	// Only include provider_id and event_type for trigger nodes
	if node.Category == models.CategoryTypeTrigger {
		response.ProviderID = node.ProviderID
		response.EventType = node.EventType
	}

	return response
}
