package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/notify"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/registry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Simple test workflow repository implementation with new interface.
type testWorkflowRepository struct {
	workflows map[string]*models.Workflow
}

func (r *testWorkflowRepository) GetAll(ctx context.Context, tenantID string) ([]*models.Workflow, error) {
	workflows := make([]*models.Workflow, 0, len(r.workflows))
	for _, w := range r.workflows {
		if w.TenantID == tenantID {
			workflows = append(workflows, w)
		}
	}

	return workflows, nil
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

func (r *testWorkflowRepository) GetWorkflowVersions(ctx context.Context, workflowGroupID string) ([]*models.Workflow, error) {
	var versions []*models.Workflow

	for _, w := range r.workflows {
		if w.WorkflowGroupID == workflowGroupID {
			versions = append(versions, w)
		}
	}

	return versions, nil
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

func (p *testPersistence) ExecutionContextRepository() persistence.ExecutionContextRepository {
	return nil
}

func createTestPersistence() *testPersistence {
	return &testPersistence{
		workflowRepo: &testWorkflowRepository{
			workflows: make(map[string]*models.Workflow),
		},
	}
}

type testCatalog map[string]error

func (c testCatalog) Get(_ context.Context, key string) (*models.EventDefinition, error) {
	if err, ok := c[key]; ok {
		return nil, err
	}

	return &models.EventDefinition{Key: key}, nil
}

func newTestValidator() *engine.Engine {
	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes(notify.SMTP{})

	return engine.New(reg)
}

func TestPublishingService_PublishWorkflow_Success(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishingService(persistence, newTestValidator(), testCatalog{})

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
				Type:     "trigger:webhook",
				Category: models.CategoryTypeTrigger,
				Config:   map[string]any{"webhook_path": "/hook"},
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

func TestPublishingService_PublishWorkflow_ValidationError(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishingService(persistence, newTestValidator(), testCatalog{})

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

func TestPublishingService_PublishWorkflow_RejectsBrokenGraph(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishingService(persistence, newTestValidator(), testCatalog{})

	workflow := &models.Workflow{
		ID:              "broken-workflow",
		Name:            "Broken Workflow",
		Description:     "Test description",
		Status:          models.WorkflowStatusDraft,
		WorkflowGroupID: "test-group",
		Nodes: []*models.WorkflowNode{
			{
				ID:       "trigger-1",
				Type:     "trigger:webhook",
				Category: models.CategoryTypeTrigger,
				Config:   map[string]any{"webhook_path": "/hook"},
				Enabled:  true,
			},
			{
				ID:      "log-1",
				Type:    "log",
				Config:  map[string]any{"message": "done"},
				Enabled: true,
			},
		},
		Connections: []*models.Connection{
			{ID: "conn-1", SourcePort: "trigger-1:output", TargetPort: "log-1:main"},
		},
	}

	err := persistence.workflowRepo.Save(context.Background(), workflow)
	require.NoError(t, err)

	_, err = service.PublishWorkflow(context.Background(), "broken-workflow")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `output port "trigger-1:output" does not exist`)
	assert.Equal(t, models.WorkflowStatusDraft, persistence.workflowRepo.workflows["broken-workflow"].Status)
}

func TestPublishingService_GetPublishedWorkflow(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishingService(persistence, newTestValidator(), testCatalog{})

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

func TestPublishingService_CreateDraftFromPublished(t *testing.T) {
	persistence := createTestPersistence()
	service := NewPublishingService(persistence, newTestValidator(), testCatalog{})

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

func eventWorkflow(id, event string) *models.Workflow {
	return &models.Workflow{
		ID:              id,
		Name:            "Employee Criado",
		Status:          models.WorkflowStatusDraft,
		WorkflowGroupID: id,
		Nodes: []*models.WorkflowNode{
			{ID: "on-employee", Type: models.NodeTypeTriggerEvent, Category: models.CategoryTypeTrigger, Config: map[string]any{"event": event}, Enabled: true},
		},
	}
}

func TestPublishingService_PublishesEventTriggersOfTheCatalog(t *testing.T) {
	store := createTestPersistence()
	service := NewPublishingService(store, newTestValidator(), testCatalog{})
	require.NoError(t, store.workflowRepo.Save(t.Context(), eventWorkflow("hire", "employee_created")))

	published, err := service.PublishWorkflow(t.Context(), "hire")
	require.NoError(t, err)
	assert.Equal(t, models.WorkflowStatusPublished, published.Status)
}

func TestPublishingService_RejectsEventsMissingFromTheCatalog(t *testing.T) {
	store := createTestPersistence()
	service := NewPublishingService(store, newTestValidator(), testCatalog{"employee_fired": persistence.ErrEventDefinitionNotFound})
	require.NoError(t, store.workflowRepo.Save(t.Context(), eventWorkflow("fire", "employee_fired")))

	_, err := service.PublishWorkflow(t.Context(), "fire")
	require.ErrorIs(t, err, ErrInvalidWorkflow)
	assert.Contains(t, err.Error(), `node "on-employee": event "employee_fired" is not in the catalog`)
	assert.Equal(t, models.WorkflowStatusDraft, store.workflowRepo.workflows["fire"].Status)
}

func TestPublishingService_ReturnsCatalogErrors(t *testing.T) {
	errDown := errors.New("catalog down")
	store := createTestPersistence()
	service := NewPublishingService(store, newTestValidator(), testCatalog{"employee_created": errDown})
	require.NoError(t, store.workflowRepo.Save(t.Context(), eventWorkflow("hire", "employee_created")))

	_, err := service.PublishWorkflow(t.Context(), "hire")
	require.ErrorIs(t, err, errDown)
	assert.NotErrorIs(t, err, ErrInvalidWorkflow)
}
