// Package web provides HTTP request and response types for the workflow API.
package web

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
