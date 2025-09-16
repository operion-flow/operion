package services

import (
	"context"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Simple test workflow repository implementation with new interface.
type testWorkflowRepository struct {
	workflows map[string]*models.Workflow
}

func (r *testWorkflowRepository) ListWorkflows(ctx context.Context, opts persistence.ListWorkflowsOptions) (*persistence.WorkflowListResult, error) {
	// Get all workflows from map
	allWorkflows := make([]*models.Workflow, 0, len(r.workflows))
	for _, w := range r.workflows {
		allWorkflows = append(allWorkflows, w)
	}

	// Apply filtering
	filteredWorkflows := make([]*models.Workflow, 0)

	for _, workflow := range allWorkflows {
		// Owner filter
		if opts.OwnerID != "" && workflow.Owner != opts.OwnerID {
			continue
		}

		// Status filter
		if opts.Status != nil && workflow.Status != *opts.Status {
			continue
		}

		filteredWorkflows = append(filteredWorkflows, workflow)
	}

	// Apply simple pagination (for tests)
	totalCount := int64(len(filteredWorkflows))

	// Set defaults for tests
	if opts.Limit <= 0 {
		opts.Limit = 20
	}

	startIdx := opts.Offset
	endIdx := opts.Offset + opts.Limit

	if startIdx >= len(filteredWorkflows) {
		return &persistence.WorkflowListResult{
			Workflows:   make([]*models.Workflow, 0),
			TotalCount:  totalCount,
			HasNextPage: false,
		}, nil
	}

	if endIdx > len(filteredWorkflows) {
		endIdx = len(filteredWorkflows)
	}

	paginatedWorkflows := filteredWorkflows[startIdx:endIdx]
	hasNextPage := endIdx < len(filteredWorkflows)

	return &persistence.WorkflowListResult{
		Workflows:   paginatedWorkflows,
		TotalCount:  totalCount,
		HasNextPage: hasNextPage,
	}, nil
}

func (r *testWorkflowRepository) Save(ctx context.Context, workflow *models.Workflow) error {
	r.workflows[workflow.ID] = workflow

	return nil
}

func (r *testWorkflowRepository) GetByID(ctx context.Context, id string) (*models.Workflow, error) {
	if workflow, exists := r.workflows[id]; exists {
		return workflow, nil
	}

	return nil, nil
}

func (r *testWorkflowRepository) Delete(ctx context.Context, id string) error {
	delete(r.workflows, id)

	return nil
}

func (r *testWorkflowRepository) GetCurrentWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	// Try published first, then draft
	published, _ := r.GetPublishedWorkflow(ctx, workflowGroupID)
	if published != nil {
		return published, nil
	}

	return r.GetDraftWorkflow(ctx, workflowGroupID)
}

func (r *testWorkflowRepository) GetDraftWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	for _, w := range r.workflows {
		if w.WorkflowGroupID == workflowGroupID && w.Status == models.WorkflowStatusDraft {
			return w, nil
		}
	}

	return nil, nil
}

func (r *testWorkflowRepository) GetPublishedWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	for _, w := range r.workflows {
		if w.WorkflowGroupID == workflowGroupID && w.Status == models.WorkflowStatusPublished {
			return w, nil
		}
	}

	return nil, nil
}

func (r *testWorkflowRepository) PublishWorkflow(ctx context.Context, workflowID string) error {
	workflow, exists := r.workflows[workflowID]
	if !exists {
		return nil // workflow not found
	}

	// Set all other workflows in group to unpublished
	for _, w := range r.workflows {
		if w.WorkflowGroupID == workflow.WorkflowGroupID && w.Status == models.WorkflowStatusPublished {
			w.Status = models.WorkflowStatusUnpublished
		}
	}

	// Set current workflow to published
	workflow.Status = models.WorkflowStatusPublished
	if workflow.PublishedAt == nil {
		now := time.Now().UTC()
		workflow.PublishedAt = &now
	}

	return nil
}

func (r *testWorkflowRepository) CreateDraftFromPublished(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	// Check if draft already exists
	draft, _ := r.GetDraftWorkflow(ctx, workflowGroupID)
	if draft != nil {
		return draft, nil
	}

	// Get published workflow
	published, err := r.GetPublishedWorkflow(ctx, workflowGroupID)
	if err != nil || published == nil {
		return nil, err
	}

	// Create draft copy
	draftWorkflow := *published
	draftWorkflow.ID = uuid.New().String()
	draftWorkflow.Status = models.WorkflowStatusDraft
	draftWorkflow.CreatedAt = time.Now().UTC()
	draftWorkflow.UpdatedAt = time.Now().UTC()
	draftWorkflow.PublishedAt = nil

	r.workflows[draftWorkflow.ID] = &draftWorkflow

	return &draftWorkflow, nil
}

// Simple test persistence implementation.
type testPersistence struct {
	workflowRepo *testWorkflowRepository
}

func (p *testPersistence) HealthCheck(ctx context.Context) error { return nil }
func (p *testPersistence) Close(ctx context.Context) error       { return nil }

func (p *testPersistence) WorkflowRepository() persistence.WorkflowRepository {
	return p.workflowRepo
}

func (p *testPersistence) NodeRepository() persistence.NodeRepository             { return nil }
func (p *testPersistence) ConnectionRepository() persistence.ConnectionRepository { return nil }
func (p *testPersistence) ExecutionContextRepository() persistence.ExecutionContextRepository {
	return nil
}
func (p *testPersistence) InputCoordinationRepository() persistence.InputCoordinationRepository {
	return nil
}

func createTestPersistence() *testPersistence {
	return &testPersistence{
		workflowRepo: &testWorkflowRepository{
			workflows: make(map[string]*models.Workflow),
		},
	}
}

func TestPublishing_PublishWorkflow_Success(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishing(persistence)

	// Create a valid draft workflow
	workflow := &models.Workflow{
		ID:              "test-workflow",
		Name:            "Test Workflow",
		Description:     "Test description",
		Status:          models.WorkflowStatusDraft,
		WorkflowGroupID: "test-group",
		Nodes: []*models.WorkflowNode{
			{
				ID:       "trigger-1",
				Category: models.CategoryTypeTrigger,
				Enabled:  true,
			},
		},
	}

	// Save the draft workflow
	err := persistence.workflowRepo.Save(context.Background(), workflow)
	require.NoError(t, err)

	// Publish the workflow
	published, err := service.PublishWorkflow(context.Background(), "test-workflow")
	require.NoError(t, err)
	require.NotNil(t, published)

	// Verify the workflow is published
	assert.Equal(t, models.WorkflowStatusPublished, published.Status)
	assert.NotNil(t, published.PublishedAt)
}

func TestPublishing_PublishWorkflow_ValidationError(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishing(persistence)

	// Create an invalid workflow (no trigger nodes)
	workflow := &models.Workflow{
		ID:              "invalid-workflow",
		Name:            "Invalid Workflow",
		Description:     "Test description",
		Status:          models.WorkflowStatusDraft,
		WorkflowGroupID: "test-group",
		Nodes:           []*models.WorkflowNode{}, // No trigger nodes
	}

	// Save the invalid workflow
	err := persistence.workflowRepo.Save(context.Background(), workflow)
	require.NoError(t, err)

	// Try to publish - should fail validation
	_, err = service.PublishWorkflow(context.Background(), "invalid-workflow")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have at least one node")
}

func TestPublishing_GetPublishedWorkflow(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishing(persistence)

	// Create and save a published workflow
	workflow := &models.Workflow{
		ID:              "published-workflow",
		Name:            "Published Workflow",
		Status:          models.WorkflowStatusPublished,
		WorkflowGroupID: "test-group",
	}

	err := persistence.workflowRepo.Save(context.Background(), workflow)
	require.NoError(t, err)

	// Test getting published workflow
	retrieved, err := service.GetPublishedWorkflow(context.Background(), "test-group")
	require.NoError(t, err)
	assert.Equal(t, "published-workflow", retrieved.ID)
	assert.Equal(t, models.WorkflowStatusPublished, retrieved.Status)
}

func TestPublishing_CreateDraftFromPublished(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishing(persistence)

	// Create and save a published workflow
	published := &models.Workflow{
		ID:              "published-workflow",
		Name:            "Published Workflow",
		Status:          models.WorkflowStatusPublished,
		WorkflowGroupID: "test-group",
		Nodes:           []*models.WorkflowNode{{ID: "node1", Name: "Test Node"}},
	}

	err := persistence.workflowRepo.Save(context.Background(), published)
	require.NoError(t, err)

	// Create draft from published
	draft, err := service.CreateDraftFromPublished(context.Background(), "test-group")
	require.NoError(t, err)
	require.NotNil(t, draft)

	// Verify draft properties
	assert.Equal(t, models.WorkflowStatusDraft, draft.Status)
	assert.Equal(t, "test-group", draft.WorkflowGroupID)
	assert.NotEqual(t, published.ID, draft.ID) // Should have different ID
	assert.Nil(t, draft.PublishedAt)
	assert.Len(t, draft.Nodes, 1) // Should copy nodes
}
