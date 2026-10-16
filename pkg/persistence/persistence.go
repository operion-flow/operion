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
	NodeRepository() NodeRepository
	ConnectionRepository() ConnectionRepository
	ExecutionContextRepository() ExecutionContextRepository
	InputCoordinationRepository() InputCoordinationRepository

	Close(ctx context.Context) error
}

// WorkflowRepository provides workflow-related persistence operations.
type WorkflowRepository interface {
	// Basic CRUD operations
	GetAll(ctx context.Context) ([]*models.Workflow, error)
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

// NodeRepository provides access to workflow node data.
type NodeRepository interface {
	// Get nodes from a workflow
	GetNodesByWorkflow(ctx context.Context, workflowID string) ([]*models.WorkflowNode, error)
	GetNodeByWorkflow(ctx context.Context, workflowID, nodeID string) (*models.WorkflowNode, error)

	// Node CRUD operations
	SaveNode(ctx context.Context, workflowID string, node *models.WorkflowNode) error
	UpdateNode(ctx context.Context, workflowID string, node *models.WorkflowNode) error
	DeleteNode(ctx context.Context, workflowID, nodeID string) error

	// Trigger node operations
	FindTriggerNodesBySourceEventAndProvider(ctx context.Context, sourceID, eventType, providerID string, status models.WorkflowStatus) ([]*models.TriggerNodeMatch, error)
}

// ConnectionRepository provides access to workflow connection data.
type ConnectionRepository interface {
	// Get connections from a workflow with optional filtering
	GetConnectionsByWorkflow(ctx context.Context, workflowID string) ([]*models.Connection, error)
	GetConnectionsBySourceNode(ctx context.Context, workflowID, sourceNodeID string) ([]*models.Connection, error)
	GetConnectionsByTargetNode(ctx context.Context, workflowID, targetNodeID string) ([]*models.Connection, error)

	// Connection CRUD operations
	SaveConnection(ctx context.Context, workflowID string, connection *models.Connection) error
	UpdateConnection(ctx context.Context, workflowID string, connection *models.Connection) error
	DeleteConnection(ctx context.Context, workflowID, connectionID string) error
}

// ExecutionContextRepository provides access to execution context data.
type ExecutionContextRepository interface {
	SaveExecutionContext(ctx context.Context, execCtx *models.ExecutionContext) error
	GetExecutionContext(ctx context.Context, executionID string) (*models.ExecutionContext, error)
	UpdateExecutionContext(ctx context.Context, execCtx *models.ExecutionContext) error
	GetExecutionsByWorkflow(ctx context.Context, workflowID string) ([]*models.ExecutionContext, error)
	GetExecutionsByStatus(ctx context.Context, status models.ExecutionStatus) ([]*models.ExecutionContext, error)
}
