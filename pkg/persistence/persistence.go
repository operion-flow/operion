// Package persistence provides data storage abstraction layer for workflows, nodes, and execution context.
package persistence

import (
	"context"
	"errors"

	"github.com/dukex/operion/pkg/models"
)

var (
	ErrExecutionNotFound       = errors.New("execution context not found")
	ErrEventDefinitionNotFound = errors.New("event definition not found")
)

type Persistence interface {
	// Health check
	HealthCheck(ctx context.Context) error

	// Repository access
	WorkflowRepository() WorkflowRepository
	ExecutionContextRepository() ExecutionContextRepository

	Close(ctx context.Context) error
}

// WorkflowRepository provides workflow-related persistence operations.
type WorkflowRepository interface {
	// Basic CRUD operations
	GetAll(ctx context.Context, tenantID string) ([]*models.Workflow, error)
	Save(ctx context.Context, workflow *models.Workflow) error
	GetByID(ctx context.Context, id string) (*models.Workflow, error)
	Delete(ctx context.Context, id string) error

	// Simplified versioning methods
	GetWorkflowVersions(ctx context.Context, workflowGroupID string) ([]*models.Workflow, error)
	GetCurrentWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) // draft or published
	GetDraftWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error)   // draft only
	GetPublishedWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error)
	PublishWorkflow(ctx context.Context, workflowID string) error                                   // NEW: handle publish logic in repo
	CreateDraftFromPublished(ctx context.Context, workflowGroupID string) (*models.Workflow, error) // NEW: create draft copy
}

// ExecutionContextRepository provides access to execution context data.
type ExecutionContextRepository interface {
	SaveExecutionContext(ctx context.Context, execCtx *models.ExecutionContext) error
	GetExecutionContext(ctx context.Context, executionID string) (*models.ExecutionContext, error)
	UpdateExecutionContext(ctx context.Context, execCtx *models.ExecutionContext) error
	GetExecutionsByWorkflow(ctx context.Context, workflowID string, limit int) ([]*models.ExecutionContext, error)
	GetExecutionsByStatus(ctx context.Context, status models.ExecutionStatus) ([]*models.ExecutionContext, error)
}
