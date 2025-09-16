// Package persistence provides standardized error types for persistence operations.
package persistence

import (
	"errors"
	"fmt"
)

// Standard persistence error types that all implementations should use.
var (
	// ErrWorkflowNotFound indicates a workflow was not found by the given identifier.
	ErrWorkflowNotFound = errors.New("workflow not found")

	// ErrPublishedWorkflowNotFound indicates no published workflow exists for the given group.
	ErrPublishedWorkflowNotFound = errors.New("published workflow not found")

	// ErrDraftWorkflowNotFound indicates no draft workflow exists for the given group.
	ErrDraftWorkflowNotFound = errors.New("draft workflow not found")

	// ErrWorkflowAlreadyExists indicates a workflow with the same identifier already exists.
	ErrWorkflowAlreadyExists = errors.New("workflow already exists")

	// ErrInvalidWorkflowStatus indicates an invalid workflow status was provided.
	ErrInvalidWorkflowStatus = errors.New("invalid workflow status")

	// ErrNodeNotFound indicates a node was not found by the given identifier.
	ErrNodeNotFound = errors.New("node not found")

	// ErrConnectionNotFound indicates a connection was not found by the given identifier.
	ErrConnectionNotFound = errors.New("connection not found")

	// ErrExecutionContextNotFound indicates an execution context was not found.
	ErrExecutionContextNotFound = errors.New("execution context not found")

	// Validation errors that indicate client errors (internal to persistence layer)
	// ErrInvalidSortField indicates an invalid sort field was provided.
	ErrInvalidSortField = errors.New("invalid sort field")

	// ErrInvalidPortFormat indicates an invalid port format was provided.
	ErrInvalidPortFormat = errors.New("invalid port format")
)

// WorkflowError wraps workflow-related errors with additional context.
type WorkflowError struct {
	Op         string // Operation being performed (e.g., "GetByID", "Save", "Delete")
	WorkflowID string // Workflow ID if applicable
	GroupID    string // Workflow group ID if applicable
	Err        error  // Underlying error
	Message    string // Additional context message
}

func (e *WorkflowError) Error() string {
	target := e.WorkflowID
	if e.GroupID != "" {
		target = "group " + e.GroupID
	}

	if e.Message != "" {
		return fmt.Sprintf("%s operation failed for workflow %s: %s (%v)", e.Op, target, e.Message, e.Err)
	}

	return fmt.Sprintf("%s operation failed for workflow %s: %v", e.Op, target, e.Err)
}

func (e *WorkflowError) Unwrap() error {
	return e.Err
}

// Is implements error comparison for workflow errors.
func (e *WorkflowError) Is(target error) bool {
	return errors.Is(e.Err, target)
}

// NewWorkflowError creates a new workflow error with context.
func NewWorkflowError(op, workflowID string, err error) *WorkflowError {
	return &WorkflowError{
		Op:         op,
		WorkflowID: workflowID,
		Err:        err,
	}
}

// NewWorkflowGroupError creates a new workflow error for group operations.
func NewWorkflowGroupError(op, groupID string, err error) *WorkflowError {
	return &WorkflowError{
		Op:      op,
		GroupID: groupID,
		Err:     err,
	}
}

// NodeError wraps node-related errors with additional context.
type NodeError struct {
	Op         string // Operation being performed
	WorkflowID string // Workflow ID
	NodeID     string // Node ID
	Err        error  // Underlying error
}

func (e *NodeError) Error() string {
	return fmt.Sprintf("%s operation failed for node %s in workflow %s: %v", e.Op, e.NodeID, e.WorkflowID, e.Err)
}

func (e *NodeError) Unwrap() error {
	return e.Err
}

func (e *NodeError) Is(target error) bool {
	return errors.Is(e.Err, target)
}

// ConnectionError wraps connection-related errors with additional context.
type ConnectionError struct {
	Op           string // Operation being performed
	WorkflowID   string // Workflow ID
	ConnectionID string // Connection ID
	Err          error  // Underlying error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("%s operation failed for connection %s in workflow %s: %v", e.Op, e.ConnectionID, e.WorkflowID, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

func (e *ConnectionError) Is(target error) bool {
	return errors.Is(e.Err, target)
}

// IsWorkflowNotFound checks if an error indicates a workflow was not found.
func IsWorkflowNotFound(err error) bool {
	return errors.Is(err, ErrWorkflowNotFound)
}

// IsPublishedWorkflowNotFound checks if an error indicates a published workflow was not found.
func IsPublishedWorkflowNotFound(err error) bool {
	return errors.Is(err, ErrPublishedWorkflowNotFound)
}

// IsDraftWorkflowNotFound checks if an error indicates a draft workflow was not found.
func IsDraftWorkflowNotFound(err error) bool {
	return errors.Is(err, ErrDraftWorkflowNotFound)
}

// IsNodeNotFound checks if an error indicates a node was not found.
func IsNodeNotFound(err error) bool {
	return errors.Is(err, ErrNodeNotFound)
}

// IsConnectionNotFound checks if an error indicates a connection was not found.
func IsConnectionNotFound(err error) bool {
	return errors.Is(err, ErrConnectionNotFound)
}

// IsInvalidSortField checks if an error indicates an invalid sort field.
func IsInvalidSortField(err error) bool {
	return errors.Is(err, ErrInvalidSortField)
}

// IsInvalidPortFormat checks if an error indicates an invalid port format.
func IsInvalidPortFormat(err error) bool {
	return errors.Is(err, ErrInvalidPortFormat)
}
