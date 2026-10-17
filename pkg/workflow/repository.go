package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/google/uuid"
)

var (
	// ErrWorkflowNotFound is returned when a workflow is not found.
	ErrWorkflowNotFound = errors.New("workflow not found")
	ErrInvalidWorkflow  = errors.New("workflow validation failed")
)

type Repository struct {
	persistence persistence.Persistence
}

// NewRepository creates a new workflow repository.
func NewRepository(persistence persistence.Persistence) *Repository {
	return &Repository{
		persistence: persistence,
	}
}

// HealthCheck checks the health of the persistence layer.
func (r *Repository) HealthCheck(ctx context.Context) (string, bool) {
	if r.persistence == nil {
		return "Persistence layer not initialized", false
	}

	err := r.persistence.HealthCheck(ctx)
	if err != nil {
		return "Persistence layer is unhealthy: " + err.Error(), false
	}

	return "Persistence layer is healthy", true
}

// FetchAll retrieves all workflows.
func (r *Repository) FetchAll(ctx context.Context, tenantID string) ([]*models.Workflow, error) {
	workflows, err := r.persistence.WorkflowRepository().GetAll(ctx, tenantID)
	if err != nil {
		return make([]*models.Workflow, 0), err
	}

	return workflows, nil
}

// FetchByID retrieves a workflow by its ID.
func (r *Repository) FetchByID(ctx context.Context, id string) (*models.Workflow, error) {
	workflow, err := r.persistence.WorkflowRepository().GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if workflow == nil {
		return nil, ErrWorkflowNotFound
	}

	return workflow, nil
}

// Create adds a new workflow to the repository.
func (r *Repository) Create(ctx context.Context, workflow *models.Workflow) (*models.Workflow, error) {
	now := time.Now().UTC()
	workflow.ID = uuid.New().String()
	workflow.CreatedAt = now
	workflow.UpdatedAt = now

	if workflow.Status == "" {
		workflow.Status = models.WorkflowStatusDraft
	}

	err := r.persistence.WorkflowRepository().Save(ctx, workflow)
	if err != nil {
		return nil, err
	}

	return workflow, nil
}

// Update modifies an existing workflow by its ID.
func (r *Repository) Update(
	ctx context.Context,
	workflowID string,
	workflow *models.Workflow,
) (*models.Workflow, error) {
	existing, err := r.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	if existing == nil {
		return nil, ErrWorkflowNotFound
	}

	workflow.ID = workflowID
	workflow.CreatedAt = existing.CreatedAt
	workflow.UpdatedAt = time.Now().UTC()

	err = r.persistence.WorkflowRepository().Save(ctx, workflow)
	if err != nil {
		return nil, err
	}

	return workflow, nil
}

// Delete removes a workflow by its ID.
func (r *Repository) Delete(ctx context.Context, workflowID string) error {
	existing, err := r.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		return err
	}

	if existing == nil {
		return ErrWorkflowNotFound
	}

	err = r.persistence.WorkflowRepository().Delete(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to delete workflow: %w", err)
	}

	return nil
}
