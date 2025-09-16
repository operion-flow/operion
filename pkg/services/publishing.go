// Package services provides workflow publishing functionality with simplified versioning.
package services

import (
	"context"
	"fmt"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
)

// Publishing handles workflow publishing operations with simplified versioning.
type Publishing struct {
	persistence persistence.Persistence
}

// NewPublishing creates a new workflow publishing service.
func NewPublishing(persistence persistence.Persistence) *Publishing {
	return &Publishing{
		persistence: persistence,
	}
}

// PublishWorkflow changes a workflow's status to published and manages version history.
func (p *Publishing) PublishWorkflow(ctx context.Context, workflowID string) (*models.Workflow, error) {
	// Validate workflow can be published (same validation as before)
	workflow, err := p.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	if workflow == nil {
		return nil, persistence.NewWorkflowError("PublishWorkflow", workflowID, persistence.ErrWorkflowNotFound)
	}

	if err := p.validateForPublishing(workflow); err != nil {
		return nil, fmt.Errorf("workflow validation failed: %w", err)
	}

	// Use repository's PublishWorkflow method to handle status changes
	if err := p.persistence.WorkflowRepository().PublishWorkflow(ctx, workflowID); err != nil {
		return nil, fmt.Errorf("failed to publish workflow: %w", err)
	}

	// Return the updated workflow
	return p.persistence.WorkflowRepository().GetByID(ctx, workflowID)
}

// GetPublishedWorkflow returns the published version of a workflow group.
func (p *Publishing) GetPublishedWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	return p.persistence.WorkflowRepository().GetPublishedWorkflow(ctx, workflowGroupID)
}

// CreateDraftFromPublished creates a draft copy from published version.
func (p *Publishing) CreateDraftFromPublished(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	return p.persistence.WorkflowRepository().CreateDraftFromPublished(ctx, workflowGroupID)
}

// GetCurrentWorkflow returns the current version (published if exists, otherwise draft).
func (p *Publishing) GetCurrentWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	return p.persistence.WorkflowRepository().GetCurrentWorkflow(ctx, workflowGroupID)
}

// GetDraftWorkflow returns the draft version of a workflow group.
func (p *Publishing) GetDraftWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	return p.persistence.WorkflowRepository().GetDraftWorkflow(ctx, workflowGroupID)
}

// validateForPublishing ensures a workflow is ready to be published.
func (p *Publishing) validateForPublishing(workflow *models.Workflow) error {
	if workflow == nil {
		return ErrWorkflowNil
	}

	if workflow.Name == "" {
		return ErrWorkflowNameRequired
	}

	if len(workflow.Nodes) == 0 {
		return ErrNodesRequired
	}

	// Ensure there is at least one trigger node
	var hasTrigger bool

	for _, node := range workflow.Nodes {
		if node.Category == models.CategoryTypeTrigger && node.Enabled {
			hasTrigger = true

			break
		}
	}

	if !hasTrigger {
		return ErrTriggerNodeRequired
	}

	return nil
}
