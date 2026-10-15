// Package workflow provides workflow publishing functionality with simplified versioning.
package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
)

// PublishingService handles workflow publishing operations with simplified versioning.
type PublishingService struct {
	persistence persistence.Persistence
	validator   Validator
}

type Validator interface {
	Validate(ctx context.Context, workflow *models.Workflow) error
}

// NewPublishingService creates a new workflow publishing service.
func NewPublishingService(persistence persistence.Persistence, validator Validator) *PublishingService {
	return &PublishingService{
		persistence: persistence,
		validator:   validator,
	}
}

// PublishWorkflow changes a workflow's status to published and manages version history.
func (s *PublishingService) PublishWorkflow(ctx context.Context, workflowID string) (*models.Workflow, error) {
	// Validate workflow can be published (same validation as before)
	workflow, err := s.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	if workflow == nil {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	if err := s.validateForPublishing(workflow); err != nil {
		return nil, fmt.Errorf("workflow validation failed: %w", err)
	}

	if err := s.validator.Validate(ctx, workflow); err != nil {
		return nil, fmt.Errorf("workflow validation failed: %w", err)
	}

	// Use repository's PublishWorkflow method to handle status changes
	if err := s.persistence.WorkflowRepository().PublishWorkflow(ctx, workflowID); err != nil {
		return nil, fmt.Errorf("failed to publish workflow: %w", err)
	}

	// Return the updated workflow
	return s.persistence.WorkflowRepository().GetByID(ctx, workflowID)
}

// GetPublishedWorkflow returns the published version of a workflow group.
func (s *PublishingService) GetPublishedWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	return s.persistence.WorkflowRepository().GetPublishedWorkflow(ctx, workflowGroupID)
}

// CreateDraftFromPublished creates a draft copy from published version.
func (s *PublishingService) CreateDraftFromPublished(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	return s.persistence.WorkflowRepository().CreateDraftFromPublished(ctx, workflowGroupID)
}

// GetCurrentWorkflow returns the current version (published if exists, otherwise draft).
func (s *PublishingService) GetCurrentWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	return s.persistence.WorkflowRepository().GetCurrentWorkflow(ctx, workflowGroupID)
}

// GetDraftWorkflow returns the draft version of a workflow group.
func (s *PublishingService) GetDraftWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	return s.persistence.WorkflowRepository().GetDraftWorkflow(ctx, workflowGroupID)
}

// validateForPublishing ensures a workflow is ready to be published.
func (s *PublishingService) validateForPublishing(workflow *models.Workflow) error {
	if workflow == nil {
		return errors.New("workflow cannot be nil")
	}

	if workflow.Name == "" {
		return errors.New("workflow name cannot be empty")
	}

	if len(workflow.Nodes) == 0 {
		return errors.New("workflow must have at least one node")
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
		return errors.New("workflow must have at least one enabled trigger node")
	}

	return nil
}
