package services

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/google/uuid"
)

var (
	// ErrWorkflowNotFound is returned when a workflow is not found.
	ErrWorkflowNotFound = persistence.ErrWorkflowNotFound
)

type Workflow struct {
	persistence persistence.Persistence
}

// NewWorkflow creates a new workflow service.
func NewWorkflow(persistence persistence.Persistence) *Workflow {
	return &Workflow{
		persistence: persistence,
	}
}

// HealthCheck checks the health of the persistence layer.
func (w *Workflow) HealthCheck(ctx context.Context) (string, bool) {
	if w.persistence == nil {
		return "Persistence layer not initialized", false
	}

	err := w.persistence.HealthCheck(ctx)
	if err != nil {
		return "Persistence layer is unhealthy: " + err.Error(), false
	}

	return "Persistence layer is healthy", true
}

// ListWorkflowsRequest contains options for listing workflows.
type ListWorkflowsRequest struct {
	// Pagination
	Page    int `validate:"min=1"`
	PerPage int `validate:"min=1,max=100"`

	// Filtering
	OwnerID string
	Status  *models.WorkflowStatus

	// Sorting
	SortBy    string `validate:"oneof=created_at updated_at name"`
	SortOrder string `validate:"oneof=asc desc"`

	// Data Loading Control
	IncludeNodes       bool
	IncludeConnections bool
}

// ListWorkflowsResponse contains the result of listing workflows.
type ListWorkflowsResponse struct {
	Workflows   []*models.Workflow `json:"workflows"`
	TotalCount  int64              `json:"total_count"`
	HasNextPage bool               `json:"has_next_page"`
}

// ListWorkflows retrieves workflows with filtering, sorting, and pagination.
func (w *Workflow) ListWorkflows(ctx context.Context, req *ListWorkflowsRequest) (*ListWorkflowsResponse, error) {
	// Validate and set defaults
	if err := w.validateListWorkflowsRequest(req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	// Convert page-based pagination to limit/offset for persistence layer
	limit := req.PerPage
	offset := (req.Page - 1) * req.PerPage

	// Convert to persistence options
	opts := persistence.ListWorkflowsOptions{
		Limit:              limit,
		Offset:             offset,
		OwnerID:            req.OwnerID,
		Status:             req.Status,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		IncludeNodes:       req.IncludeNodes,
		IncludeConnections: req.IncludeConnections,
	}

	// Call persistence layer
	result, err := w.persistence.WorkflowRepository().ListWorkflows(ctx, opts)
	if err != nil {
		// Map persistence validation errors to service validation errors
		if persistence.IsInvalidSortField(err) {
			return nil, ErrInvalidSortField // CHANGED: Map to service error
		}

		if persistence.IsInvalidPortFormat(err) {
			return nil, ErrInvalidConnectionData // CHANGED: Map to service error
		}

		// Other persistence errors remain as 500s
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}

	// Convert to service response
	return &ListWorkflowsResponse{
		Workflows:   result.Workflows,
		TotalCount:  result.TotalCount,
		HasNextPage: result.HasNextPage,
	}, nil
}

// validateListWorkflowsRequest validates and sets defaults for the request.
func (w *Workflow) validateListWorkflowsRequest(req *ListWorkflowsRequest) error {
	// Set defaults
	if req.PerPage <= 0 {
		req.PerPage = 20
	}

	if req.PerPage > 100 {
		req.PerPage = 100
	}

	if req.Page <= 0 {
		req.Page = 1
	}

	if req.SortBy == "" {
		req.SortBy = "created_at"
	}

	if req.SortOrder == "" {
		req.SortOrder = "desc"
	}

	// Validate sort parameters against allowlist
	allowedSorts := []string{"created_at", "updated_at", "name"}

	if !slices.Contains(allowedSorts, req.SortBy) {
		return NewValidationError(
			"validateListWorkflowsRequest",
			"INVALID_SORT_FIELD",
			fmt.Sprintf("invalid sort field '%s', allowed: %s", req.SortBy, strings.Join(allowedSorts, ", ")),
			ErrInvalidSortField,
		)
	}

	// Validate sort order
	if req.SortOrder != "asc" && req.SortOrder != "desc" {
		return NewValidationError(
			"validateListWorkflowsRequest",
			"INVALID_SORT_ORDER",
			fmt.Sprintf("invalid sort order '%s', allowed: asc, desc", req.SortOrder),
			ErrInvalidSortOrder,
		)
	}

	// Validate status if provided
	if req.Status != nil {
		allowedStatuses := []models.WorkflowStatus{
			models.WorkflowStatusDraft,
			models.WorkflowStatusPublished,
			models.WorkflowStatusUnpublished,
		}

		if !slices.Contains(allowedStatuses, *req.Status) {
			return NewValidationError(
				"validateListWorkflowsRequest",
				"INVALID_STATUS",
				fmt.Sprintf("invalid status '%s'", *req.Status),
				ErrInvalidStatus,
			)
		}
	}

	// Validate OwnerID format if provided (basic validation)
	if req.OwnerID != "" {
		req.OwnerID = strings.TrimSpace(req.OwnerID)
		if req.OwnerID == "" {
			return ErrEmptyOwnerID
		}
	}

	return nil
}

// FetchByID retrieves a workflow by its ID.
func (w *Workflow) FetchByID(ctx context.Context, id string) (*models.Workflow, error) {
	workflow, err := w.persistence.WorkflowRepository().GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if workflow == nil {
		return nil, ErrWorkflowNotFound
	}

	return workflow, nil
}

// Create adds a new workflow to the repository.
func (w *Workflow) Create(ctx context.Context, workflow *models.Workflow) (*models.Workflow, error) {
	now := time.Now().UTC()
	workflow.ID = uuid.New().String()
	workflow.CreatedAt = now
	workflow.UpdatedAt = now

	if workflow.Status == "" {
		workflow.Status = models.WorkflowStatusDraft
	}

	err := w.persistence.WorkflowRepository().Save(ctx, workflow)
	if err != nil {
		// Map persistence validation errors to service validation errors
		if persistence.IsInvalidPortFormat(err) {
			return nil, ErrInvalidConnectionData
		}

		// Other persistence errors remain as 500s
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	return workflow, nil
}

// Update modifies an existing workflow.
func (w *Workflow) Update(
	ctx context.Context,
	workflow *models.Workflow,
) (*models.Workflow, error) {
	// Update timestamp
	workflow.UpdatedAt = time.Now().UTC()

	err := w.persistence.WorkflowRepository().Save(ctx, workflow)
	if err != nil {
		// Map persistence validation errors to service validation errors
		if persistence.IsInvalidPortFormat(err) {
			return nil, ErrInvalidConnectionData
		}

		// Other persistence errors remain as 500s
		return nil, fmt.Errorf("failed to update workflow: %w", err)
	}

	return workflow, nil
}

// Delete removes a workflow by its ID.
func (w *Workflow) Delete(ctx context.Context, workflowID string) error {
	existing, err := w.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		return err
	}

	if existing == nil {
		return ErrWorkflowNotFound
	}

	err = w.persistence.WorkflowRepository().Delete(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to delete workflow: %w", err)
	}

	return nil
}
