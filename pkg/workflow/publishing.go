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
	catalog     EventCatalog
}

type Validator interface {
	Validate(ctx context.Context, workflow *models.Workflow) error
}

type EventCatalog interface {
	Get(ctx context.Context, key string) (*models.EventDefinition, error)
}

// NewPublishingService creates a new workflow publishing service.
func NewPublishingService(persistence persistence.Persistence, validator Validator, catalog EventCatalog) *PublishingService {
	return &PublishingService{
		persistence: persistence,
		validator:   validator,
		catalog:     catalog,
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
		return nil, fmt.Errorf("%w: %s", ErrWorkflowNotFound, workflowID)
	}

	if err := s.validateForPublishing(workflow); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidWorkflow, err)
	}

	missing, err := s.missingEvents(ctx, workflow)
	if err != nil {
		return nil, err
	}

	if err := errors.Join(append(missing, s.validator.Validate(ctx, workflow))...); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidWorkflow, err)
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

func (s *PublishingService) missingEvents(ctx context.Context, workflow *models.Workflow) ([]error, error) {
	var missing []error

	for _, node := range workflow.Nodes {
		key, _ := node.Config["event"].(string)
		if node.Type != models.NodeTypeTriggerEvent || key == "" {
			continue
		}

		_, err := s.catalog.Get(ctx, key)

		switch {
		case errors.Is(err, persistence.ErrEventDefinitionNotFound):
			missing = append(missing, fmt.Errorf("node %q: event %q is not in the catalog", node.ID, key))
		case err != nil:
			return nil, err
		}
	}

	return missing, nil
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
