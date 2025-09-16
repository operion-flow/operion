package services

import (
	"context"
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testWorkflowID = "workflow-123"

// Mock interfaces for testing.
type MockPersistence struct {
	mock.Mock
}

func (m *MockPersistence) HealthCheck(ctx context.Context) error {
	args := m.Called(ctx)

	return args.Error(0)
}

func (m *MockPersistence) WorkflowRepository() persistence.WorkflowRepository {
	args := m.Called()

	return args.Get(0).(persistence.WorkflowRepository)
}

func (m *MockPersistence) NodeRepository() persistence.NodeRepository {
	args := m.Called()

	return args.Get(0).(persistence.NodeRepository)
}

func (m *MockPersistence) ConnectionRepository() persistence.ConnectionRepository {
	args := m.Called()

	return args.Get(0).(persistence.ConnectionRepository)
}

func (m *MockPersistence) ExecutionContextRepository() persistence.ExecutionContextRepository {
	args := m.Called()

	return args.Get(0).(persistence.ExecutionContextRepository)
}

func (m *MockPersistence) InputCoordinationRepository() persistence.InputCoordinationRepository {
	args := m.Called()

	return args.Get(0).(persistence.InputCoordinationRepository)
}

func (m *MockPersistence) Close(ctx context.Context) error {
	args := m.Called(ctx)

	return args.Error(0)
}

type MockWorkflowRepository struct {
	mock.Mock
}

func (m *MockWorkflowRepository) ListWorkflows(ctx context.Context, opts persistence.ListWorkflowsOptions) (*persistence.WorkflowListResult, error) {
	args := m.Called(ctx, opts)

	return args.Get(0).(*persistence.WorkflowListResult), args.Error(1)
}

func (m *MockWorkflowRepository) Save(ctx context.Context, workflow *models.Workflow) error {
	args := m.Called(ctx, workflow)

	return args.Error(0)
}

func (m *MockWorkflowRepository) GetByID(ctx context.Context, id string) (*models.Workflow, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Workflow), args.Error(1)
}

func (m *MockWorkflowRepository) Delete(ctx context.Context, id string) error {
	args := m.Called(ctx, id)

	return args.Error(0)
}

func (m *MockWorkflowRepository) GetCurrentWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	args := m.Called(ctx, workflowGroupID)

	return args.Get(0).(*models.Workflow), args.Error(1)
}

func (m *MockWorkflowRepository) GetDraftWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	args := m.Called(ctx, workflowGroupID)

	return args.Get(0).(*models.Workflow), args.Error(1)
}

func (m *MockWorkflowRepository) GetPublishedWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	args := m.Called(ctx, workflowGroupID)

	return args.Get(0).(*models.Workflow), args.Error(1)
}

func (m *MockWorkflowRepository) PublishWorkflow(ctx context.Context, workflowID string) error {
	args := m.Called(ctx, workflowID)

	return args.Error(0)
}

func (m *MockWorkflowRepository) CreateDraftFromPublished(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	args := m.Called(ctx, workflowGroupID)

	return args.Get(0).(*models.Workflow), args.Error(1)
}

type MockNodeRepository struct {
	mock.Mock
}

func (m *MockNodeRepository) GetNodesByWorkflow(ctx context.Context, workflowID string) ([]*models.WorkflowNode, error) {
	args := m.Called(ctx, workflowID)

	return args.Get(0).([]*models.WorkflowNode), args.Error(1)
}

func (m *MockNodeRepository) GetNodeByWorkflow(ctx context.Context, workflowID, nodeID string) (*models.WorkflowNode, error) {
	args := m.Called(ctx, workflowID, nodeID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.WorkflowNode), args.Error(1)
}

func (m *MockNodeRepository) SaveNode(ctx context.Context, workflowID string, node *models.WorkflowNode) error {
	args := m.Called(ctx, workflowID, node)

	return args.Error(0)
}

func (m *MockNodeRepository) UpdateNode(ctx context.Context, workflowID string, node *models.WorkflowNode) error {
	args := m.Called(ctx, workflowID, node)

	return args.Error(0)
}

func (m *MockNodeRepository) DeleteNode(ctx context.Context, workflowID, nodeID string) error {
	args := m.Called(ctx, workflowID, nodeID)

	return args.Error(0)
}

func (m *MockNodeRepository) DeleteNodeWithConnections(ctx context.Context, workflowID, nodeID string) error {
	args := m.Called(ctx, workflowID, nodeID)

	return args.Error(0)
}

func (m *MockNodeRepository) FindTriggerNodesBySourceEventAndProvider(ctx context.Context, sourceID, eventType, providerID string, status models.WorkflowStatus) ([]*models.TriggerNodeMatch, error) {
	args := m.Called(ctx, sourceID, eventType, providerID, status)

	return args.Get(0).([]*models.TriggerNodeMatch), args.Error(1)
}

func TestNode_CreateNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		workflowID    string
		request       *CreateNodeRequest
		setupMocks    func(*MockPersistence, *MockWorkflowRepository, *MockNodeRepository)
		expectedError string
		validateNode  func(t *testing.T, node *models.WorkflowNode)
	}{
		{
			name:       "successful creation",
			workflowID: testWorkflowID,
			request: &CreateNodeRequest{
				Type:      "log",
				Category:  "action",
				Name:      "Test Node",
				Config:    map[string]any{"message": "test"},
				Enabled:   true,
				PositionX: 100,
				PositionY: 200,
			},
			setupMocks: func(mp *MockPersistence, mwr *MockWorkflowRepository, mnr *MockNodeRepository) {
				workflow := testutil.CreateTestWorkflow()
				workflow.ID = testWorkflowID

				mp.On("WorkflowRepository").Return(mwr)
				mp.On("NodeRepository").Return(mnr)
				mwr.On("GetByID", mock.Anything, testWorkflowID).Return(workflow, nil)
				mnr.On("SaveNode", mock.Anything, testWorkflowID, mock.AnythingOfType("*models.WorkflowNode")).Return(nil)
			},
			validateNode: func(t *testing.T, node *models.WorkflowNode) {
				t.Helper()
				assert.Equal(t, "log", node.Type)
				assert.Equal(t, models.CategoryTypeAction, node.Category)
				assert.Equal(t, "Test Node", node.Name)
				assert.Equal(t, true, node.Enabled)
				assert.Equal(t, 100, node.PositionX)
				assert.Equal(t, 200, node.PositionY)
				assert.Equal(t, "test", node.Config["message"])
				assert.NotEmpty(t, node.ID)
			},
		},
		{
			name:       "workflow not found",
			workflowID: "nonexistent",
			request: &CreateNodeRequest{
				Type:     "log",
				Category: "action",
				Name:     "Test",
			},
			setupMocks: func(mp *MockPersistence, mwr *MockWorkflowRepository, mnr *MockNodeRepository) {
				mp.On("WorkflowRepository").Return(mwr)
				mwr.On("GetByID", mock.Anything, "nonexistent").Return(nil, persistence.ErrWorkflowNotFound)
			},
			expectedError: "workflow not found",
		},
		{
			name:       "cannot modify published workflow",
			workflowID: "published-workflow",
			request: &CreateNodeRequest{
				Type:     "log",
				Category: "action",
				Name:     "Test",
			},
			setupMocks: func(mp *MockPersistence, mwr *MockWorkflowRepository, mnr *MockNodeRepository) {
				workflow := testutil.CreateTestWorkflow()
				workflow.ID = "published-workflow"
				workflow.Status = models.WorkflowStatusPublished

				mp.On("WorkflowRepository").Return(mwr)
				mwr.On("GetByID", mock.Anything, "published-workflow").Return(workflow, nil)
			},
			expectedError: "cannot modify published workflow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			mockPersistence := new(MockPersistence)
			mockWorkflowRepo := new(MockWorkflowRepository)
			mockNodeRepo := new(MockNodeRepository)

			if tt.setupMocks != nil {
				tt.setupMocks(mockPersistence, mockWorkflowRepo, mockNodeRepo)
			}

			nodeService := NewNode(mockPersistence)

			node, err := nodeService.CreateNode(ctx, tt.workflowID, tt.request)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Nil(t, node)
			} else {
				require.NoError(t, err)
				require.NotNil(t, node)

				if tt.validateNode != nil {
					tt.validateNode(t, node)
				}
			}

			mockPersistence.AssertExpectations(t)
			mockWorkflowRepo.AssertExpectations(t)
			mockNodeRepo.AssertExpectations(t)
		})
	}
}

func TestNode_UpdateNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		workflowID    string
		nodeID        string
		request       *UpdateNodeRequest
		setupMocks    func(*MockPersistence, *MockWorkflowRepository, *MockNodeRepository)
		expectedError string
		validateNode  func(t *testing.T, node *models.WorkflowNode)
	}{
		{
			name:       "successful update",
			workflowID: testWorkflowID,
			nodeID:     "node-456",
			request: &UpdateNodeRequest{
				Name:      "Updated Node",
				Config:    map[string]any{"message": "updated"},
				PositionX: 150,
				PositionY: 250,
				Enabled:   false,
			},
			setupMocks: func(mp *MockPersistence, mwr *MockWorkflowRepository, mnr *MockNodeRepository) {
				workflow := testutil.CreateTestWorkflow()
				workflow.ID = testWorkflowID

				existingNode := testutil.CreateTestNode(testutil.WithID("node-456"))

				mp.On("WorkflowRepository").Return(mwr)
				mp.On("NodeRepository").Return(mnr)
				mwr.On("GetByID", mock.Anything, testWorkflowID).Return(workflow, nil)
				mnr.On("GetNodeByWorkflow", mock.Anything, testWorkflowID, "node-456").Return(existingNode, nil)
				mnr.On("UpdateNode", mock.Anything, testWorkflowID, mock.AnythingOfType("*models.WorkflowNode")).Return(nil)
			},
			validateNode: func(t *testing.T, node *models.WorkflowNode) {
				t.Helper()
				assert.Equal(t, "Updated Node", node.Name)
				assert.Equal(t, 150, node.PositionX)
				assert.Equal(t, 250, node.PositionY)
				assert.Equal(t, false, node.Enabled)
				assert.Equal(t, "updated", node.Config["message"])
				// Type and Category should be preserved
				assert.Equal(t, "log", node.Type)
				assert.Equal(t, models.CategoryTypeAction, node.Category)
			},
		},
		{
			name:       "node not found",
			workflowID: testWorkflowID,
			nodeID:     "nonexistent",
			request: &UpdateNodeRequest{
				Name: "Test",
			},
			setupMocks: func(mp *MockPersistence, mwr *MockWorkflowRepository, mnr *MockNodeRepository) {
				workflow := testutil.CreateTestWorkflow()
				workflow.ID = testWorkflowID

				mp.On("WorkflowRepository").Return(mwr)
				mp.On("NodeRepository").Return(mnr)
				mwr.On("GetByID", mock.Anything, testWorkflowID).Return(workflow, nil)
				mnr.On("GetNodeByWorkflow", mock.Anything, testWorkflowID, "nonexistent").Return(nil, assert.AnError)
			},
			expectedError: "failed to get node",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			mockPersistence := new(MockPersistence)
			mockWorkflowRepo := new(MockWorkflowRepository)
			mockNodeRepo := new(MockNodeRepository)

			if tt.setupMocks != nil {
				tt.setupMocks(mockPersistence, mockWorkflowRepo, mockNodeRepo)
			}

			nodeService := NewNode(mockPersistence)

			node, err := nodeService.UpdateNode(ctx, tt.workflowID, tt.nodeID, tt.request)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Nil(t, node)
			} else {
				require.NoError(t, err)
				require.NotNil(t, node)

				if tt.validateNode != nil {
					tt.validateNode(t, node)
				}
			}

			mockPersistence.AssertExpectations(t)
			mockWorkflowRepo.AssertExpectations(t)
			mockNodeRepo.AssertExpectations(t)
		})
	}
}

func TestNode_DeleteNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		workflowID    string
		nodeID        string
		setupMocks    func(*MockPersistence, *MockWorkflowRepository, *MockNodeRepository)
		expectedError string
	}{
		{
			name:       "successful deletion",
			workflowID: testWorkflowID,
			nodeID:     "node-456",
			setupMocks: func(mp *MockPersistence, mwr *MockWorkflowRepository, mnr *MockNodeRepository) {
				workflow := testutil.CreateTestWorkflow()
				workflow.ID = testWorkflowID

				mp.On("WorkflowRepository").Return(mwr)
				mp.On("NodeRepository").Return(mnr)
				mwr.On("GetByID", mock.Anything, testWorkflowID).Return(workflow, nil)
				mnr.On("DeleteNodeWithConnections", mock.Anything, testWorkflowID, "node-456").Return(nil)
			},
		},
		{
			name:       "workflow not found",
			workflowID: "nonexistent",
			nodeID:     "node-456",
			setupMocks: func(mp *MockPersistence, mwr *MockWorkflowRepository, mnr *MockNodeRepository) {
				mp.On("WorkflowRepository").Return(mwr)
				mwr.On("GetByID", mock.Anything, "nonexistent").Return(nil, persistence.ErrWorkflowNotFound)
			},
			expectedError: "workflow not found",
		},
		{
			name:       "cannot delete from published workflow",
			workflowID: "published-workflow",
			nodeID:     "node-456",
			setupMocks: func(mp *MockPersistence, mwr *MockWorkflowRepository, mnr *MockNodeRepository) {
				workflow := testutil.CreateTestWorkflow()
				workflow.ID = "published-workflow"
				workflow.Status = models.WorkflowStatusPublished

				mp.On("WorkflowRepository").Return(mwr)
				mwr.On("GetByID", mock.Anything, "published-workflow").Return(workflow, nil)
			},
			expectedError: "cannot modify published workflow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			mockPersistence := new(MockPersistence)
			mockWorkflowRepo := new(MockWorkflowRepository)
			mockNodeRepo := new(MockNodeRepository)

			if tt.setupMocks != nil {
				tt.setupMocks(mockPersistence, mockWorkflowRepo, mockNodeRepo)
			}

			nodeService := NewNode(mockPersistence)

			err := nodeService.DeleteNode(ctx, tt.workflowID, tt.nodeID)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}

			mockPersistence.AssertExpectations(t)
			mockWorkflowRepo.AssertExpectations(t)
			mockNodeRepo.AssertExpectations(t)
		})
	}
}

func TestNode_GetNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		workflowID string
		nodeID     string
		setupMocks func(*MockPersistence, *MockNodeRepository)
		expectErr  bool
		expectNode bool
	}{
		{
			name:       "successful retrieval",
			workflowID: testWorkflowID,
			nodeID:     "node-123",
			setupMocks: func(mp *MockPersistence, mnr *MockNodeRepository) {
				node := testutil.CreateTestNode(testutil.WithID("node-123"))
				mp.On("NodeRepository").Return(mnr)
				mnr.On("GetNodeByWorkflow", mock.Anything, testWorkflowID, "node-123").Return(node, nil)
			},
			expectErr:  false,
			expectNode: true,
		},
		{
			name:       "node not found",
			workflowID: testWorkflowID,
			nodeID:     "nonexistent",
			setupMocks: func(mp *MockPersistence, mnr *MockNodeRepository) {
				mp.On("NodeRepository").Return(mnr)
				mnr.On("GetNodeByWorkflow", mock.Anything, testWorkflowID, "nonexistent").Return(nil, assert.AnError)
			},
			expectErr:  true,
			expectNode: false,
		},
		{
			name:       "workflow not found via repository",
			workflowID: "nonexistent-workflow",
			nodeID:     "node-123",
			setupMocks: func(mp *MockPersistence, mnr *MockNodeRepository) {
				mp.On("NodeRepository").Return(mnr)
				mnr.On("GetNodeByWorkflow", mock.Anything, "nonexistent-workflow", "node-123").Return(nil, persistence.ErrWorkflowNotFound)
			},
			expectErr:  true,
			expectNode: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			mockPersistence := new(MockPersistence)
			mockNodeRepo := new(MockNodeRepository)

			if tt.setupMocks != nil {
				tt.setupMocks(mockPersistence, mockNodeRepo)
			}

			nodeService := NewNode(mockPersistence)

			node, err := nodeService.GetNode(ctx, tt.workflowID, tt.nodeID)

			if tt.expectErr {
				require.Error(t, err)
				assert.Nil(t, node)
			} else {
				require.NoError(t, err)

				if tt.expectNode {
					require.NotNil(t, node)
					assert.Equal(t, tt.nodeID, node.ID)
				}
			}

			mockPersistence.AssertExpectations(t)
			mockNodeRepo.AssertExpectations(t)
		})
	}
}

// TestNodeService_ErrorMapping tests the error mapping from persistence to service errors.
func TestNodeService_ErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		method         string
		persistenceErr error
		expectedErr    error
		shouldBeValid  bool
	}{
		{
			name:           "node not found mapping in GetNode",
			method:         "GetNode",
			persistenceErr: persistence.ErrNodeNotFound,
			expectedErr:    ErrNodeNotFound,
			shouldBeValid:  true,
		},
		{
			name:           "node not found mapping in UpdateNode",
			method:         "UpdateNode",
			persistenceErr: persistence.ErrNodeNotFound,
			expectedErr:    ErrNodeNotFound,
			shouldBeValid:  true,
		},
		{
			name:           "node not found mapping in DeleteNode",
			method:         "DeleteNode",
			persistenceErr: persistence.ErrNodeNotFound,
			expectedErr:    ErrNodeNotFound,
			shouldBeValid:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			mockPersistence := new(MockPersistence)
			mockNodeRepo := new(MockNodeRepository)
			mockWorkflowRepo := new(MockWorkflowRepository)

			nodeService := NewNode(mockPersistence)

			switch tt.method {
			case "GetNode":
				mockPersistence.On("NodeRepository").Return(mockNodeRepo)
				mockNodeRepo.On("GetNodeByWorkflow", mock.Anything, testWorkflowID, "node-123").Return(nil, tt.persistenceErr)

				_, err := nodeService.GetNode(ctx, testWorkflowID, "node-123")

				assert.ErrorIs(t, err, tt.expectedErr)

				if tt.shouldBeValid {
					assert.True(t, IsValidationError(err))
				}

			case "UpdateNode":
				// Mock workflow check first
				draftWorkflow := testutil.CreateTestWorkflow()
				draftWorkflow.ID = testWorkflowID
				draftWorkflow.Status = models.WorkflowStatusDraft

				mockPersistence.On("WorkflowRepository").Return(mockWorkflowRepo)
				mockWorkflowRepo.On("GetByID", mock.Anything, testWorkflowID).Return(draftWorkflow, nil)

				// Mock node retrieval failure
				mockPersistence.On("NodeRepository").Return(mockNodeRepo)
				mockNodeRepo.On("GetNodeByWorkflow", mock.Anything, testWorkflowID, "node-123").Return(nil, tt.persistenceErr)

				updateReq := &UpdateNodeRequest{Name: "Updated Node"}
				_, err := nodeService.UpdateNode(ctx, testWorkflowID, "node-123", updateReq)

				assert.ErrorIs(t, err, tt.expectedErr)

				if tt.shouldBeValid {
					assert.True(t, IsValidationError(err))
				}

			case "DeleteNode":
				// Mock workflow check first
				draftWorkflow := testutil.CreateTestWorkflow()
				draftWorkflow.ID = testWorkflowID
				draftWorkflow.Status = models.WorkflowStatusDraft

				mockPersistence.On("WorkflowRepository").Return(mockWorkflowRepo)
				mockWorkflowRepo.On("GetByID", mock.Anything, testWorkflowID).Return(draftWorkflow, nil)

				// Mock delete failure
				mockPersistence.On("NodeRepository").Return(mockNodeRepo)
				mockNodeRepo.On("DeleteNodeWithConnections", mock.Anything, testWorkflowID, "node-123").Return(tt.persistenceErr)

				err := nodeService.DeleteNode(ctx, testWorkflowID, "node-123")

				assert.ErrorIs(t, err, tt.expectedErr)

				if tt.shouldBeValid {
					assert.True(t, IsValidationError(err))
				}
			}

			mockPersistence.AssertExpectations(t)
			mockNodeRepo.AssertExpectations(t)
			mockWorkflowRepo.AssertExpectations(t)
		})
	}
}

// TestNodeService_WorkflowStatusValidation tests workflow status validation for node operations.
func TestNodeService_WorkflowStatusValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		workflowStatus   models.WorkflowStatus
		expectedErr      error
		shouldBeConflict bool
	}{
		{
			name:             "published workflow modification attempt",
			workflowStatus:   models.WorkflowStatusPublished,
			expectedErr:      ErrCannotModifyPublished,
			shouldBeConflict: true,
		},
		{
			name:             "unpublished workflow modification attempt",
			workflowStatus:   models.WorkflowStatusUnpublished,
			expectedErr:      ErrCannotModifyUnpublished,
			shouldBeConflict: true,
		},
		{
			name:             "draft workflow allows modification",
			workflowStatus:   models.WorkflowStatusDraft,
			expectedErr:      nil,
			shouldBeConflict: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			mockPersistence := new(MockPersistence)
			mockNodeRepo := new(MockNodeRepository)
			mockWorkflowRepo := new(MockWorkflowRepository)

			nodeService := NewNode(mockPersistence)

			// Setup workflow with specific status
			workflow := testutil.CreateTestWorkflow()
			workflow.ID = testWorkflowID
			workflow.Status = tt.workflowStatus

			mockPersistence.On("WorkflowRepository").Return(mockWorkflowRepo)
			mockWorkflowRepo.On("GetByID", mock.Anything, testWorkflowID).Return(workflow, nil)

			if tt.expectedErr == nil {
				// For draft workflow, also mock successful node creation
				mockPersistence.On("NodeRepository").Return(mockNodeRepo)
				mockNodeRepo.On("SaveNode", mock.Anything, testWorkflowID, mock.AnythingOfType("*models.WorkflowNode")).Return(nil)
			}

			// Test CreateNode operation
			createReq := &CreateNodeRequest{
				Type:     "log",
				Category: "action",
				Name:     "Test Node",
				Enabled:  true,
			}

			_, err := nodeService.CreateNode(ctx, testWorkflowID, createReq)

			if tt.expectedErr != nil {
				assert.ErrorIs(t, err, tt.expectedErr)

				if tt.shouldBeConflict {
					assert.True(t, IsConflictError(err))
				}
			} else {
				assert.NoError(t, err)
			}

			mockPersistence.AssertExpectations(t)
			mockWorkflowRepo.AssertExpectations(t)
			mockNodeRepo.AssertExpectations(t)
		})
	}
}

// TestNodeService_IsValidationError tests the IsValidationError function includes node errors.
func TestNodeService_IsValidationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ErrNodeNotFound should be validation error",
			err:      ErrNodeNotFound,
			expected: true,
		},
		{
			name:     "ErrCannotModifyPublished should not be validation error",
			err:      ErrCannotModifyPublished,
			expected: false,
		},
		{
			name:     "ErrCannotModifyUnpublished should not be validation error",
			err:      ErrCannotModifyUnpublished,
			expected: false,
		},
		{
			name:     "ErrInvalidRequest should be validation error",
			err:      ErrInvalidRequest,
			expected: true,
		},
		{
			name:     "Generic error should not be validation error",
			err:      assert.AnError,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := IsValidationError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
