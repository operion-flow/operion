// Package services provides standardized error types for service layer operations.
package services

import (
	"errors"
	"fmt"
)

// Business Logic Errors - These indicate client errors (4xx responses).
var (
	// Validation Errors (400 Bad Request).
	ErrInvalidRequest   = errors.New("invalid request")
	ErrInvalidSortField = errors.New("invalid sort field")
	ErrInvalidSortOrder = errors.New("invalid sort order")
	ErrInvalidStatus    = errors.New("invalid workflow status")
	ErrEmptyOwnerID     = errors.New("owner ID cannot be empty")

	// Publishing Validation Errors (400 Bad Request).
	ErrWorkflowNameRequired = errors.New("workflow name is required")
	ErrNodesRequired        = errors.New("workflow must have at least one node")
	ErrTriggerNodeRequired  = errors.New("workflow must have at least one enabled trigger node")
	ErrWorkflowNil          = errors.New("workflow cannot be nil")

	// Connection/Port validation errors.
	ErrInvalidConnectionData = errors.New("invalid connection data")
)

// ServiceError wraps service-level errors with additional context.
type ServiceError struct {
	Op      string // Operation name
	Code    string // Error code for API responses
	Message string // Human-readable message
	Err     error  // Underlying error
}

func (e *ServiceError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Op, e.Message)
	}

	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *ServiceError) Unwrap() error {
	return e.Err
}

func (e *ServiceError) Is(target error) bool {
	return errors.Is(e.Err, target)
}

// IsValidationError checks if an error is a validation error that should return HTTP 400.
func IsValidationError(err error) bool {
	return errors.Is(err, ErrInvalidRequest) ||
		errors.Is(err, ErrInvalidSortField) ||
		errors.Is(err, ErrInvalidSortOrder) ||
		errors.Is(err, ErrInvalidStatus) ||
		errors.Is(err, ErrEmptyOwnerID) ||
		errors.Is(err, ErrWorkflowNameRequired) ||
		errors.Is(err, ErrNodesRequired) ||
		errors.Is(err, ErrTriggerNodeRequired) ||
		errors.Is(err, ErrWorkflowNil) ||
		errors.Is(err, ErrInvalidConnectionData)
}

// NewValidationError creates a new validation error with context.
func NewValidationError(op, code, message string, err error) *ServiceError {
	return &ServiceError{
		Op:      op,
		Code:    code,
		Message: message,
		Err:     err,
	}
}
