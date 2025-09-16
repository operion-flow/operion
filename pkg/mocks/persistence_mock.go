package mocks

import (
	"context"
	"errors"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/mock"
)

// MockWorkflowRepository is a mock implementation of persistence.WorkflowRepository interface.
type MockWorkflowRepository struct {
	mock.Mock
}

func (m *MockWorkflowRepository) ListWorkflows(ctx context.Context, opts persistence.ListWorkflowsOptions) (*persistence.WorkflowListResult, error) {
	args := m.Called(ctx, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

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
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Workflow), args.Error(1)
}

func (m *MockWorkflowRepository) GetDraftWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	args := m.Called(ctx, workflowGroupID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Workflow), args.Error(1)
}

func (m *MockWorkflowRepository) GetPublishedWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	args := m.Called(ctx, workflowGroupID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Workflow), args.Error(1)
}

func (m *MockWorkflowRepository) PublishWorkflow(ctx context.Context, workflowID string) error {
	args := m.Called(ctx, workflowID)

	return args.Error(0)
}

func (m *MockWorkflowRepository) CreateDraftFromPublished(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	args := m.Called(ctx, workflowGroupID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Workflow), args.Error(1)
}

// MockPersistence is a mock implementation of persistence.Persistence interface.
type MockPersistence struct {
	mock.Mock

	workflowRepo         *MockWorkflowRepository
	nodeRepo             *MockNodeRepository
	executionContextRepo *MockExecutionContextRepository
}

// NewMockPersistence creates a new MockPersistence with all mock repositories.
func NewMockPersistence() *MockPersistence {
	return &MockPersistence{
		workflowRepo:         &MockWorkflowRepository{},
		nodeRepo:             &MockNodeRepository{},
		executionContextRepo: &MockExecutionContextRepository{},
	}
}

// GetMockWorkflowRepository returns the underlying mock workflow repository for setting up expectations.
func (m *MockPersistence) GetMockWorkflowRepository() *MockWorkflowRepository {
	return m.workflowRepo
}

func (m *MockPersistence) WorkflowRepository() persistence.WorkflowRepository {
	return m.workflowRepo
}

func (m *MockPersistence) HealthCheck(ctx context.Context) error {
	args := m.Called(ctx)

	return args.Error(0)
}

func (m *MockPersistence) Close(ctx context.Context) error {
	args := m.Called(ctx)

	return args.Error(0)
}

// Node-based repository implementations (stub implementations for testing during transition).
// GetMockNodeRepository returns the underlying mock node repository for setting up expectations.
func (m *MockPersistence) GetMockNodeRepository() *MockNodeRepository {
	return m.nodeRepo
}

func (m *MockPersistence) NodeRepository() persistence.NodeRepository {
	return m.nodeRepo
}

func (m *MockPersistence) GetMockExecutionContextRepository() *MockExecutionContextRepository {
	return m.executionContextRepo
}

func (m *MockPersistence) ConnectionRepository() persistence.ConnectionRepository {
	return &MockConnectionRepository{}
}

func (m *MockPersistence) ExecutionContextRepository() persistence.ExecutionContextRepository {
	return m.executionContextRepo
}

func (m *MockPersistence) InputCoordinationRepository() persistence.InputCoordinationRepository {
	return &MockInputCoordinationRepository{}
}

// Stub mock repository implementations (not fully implemented during transition)

type MockNodeRepository struct {
	mock.Mock
}

func (nr *MockNodeRepository) GetNodesByWorkflow(ctx context.Context, workflowID string) ([]*models.WorkflowNode, error) {
	return nil, errors.New("mock node repository not implemented during transition")
}

func (nr *MockNodeRepository) GetNodeByWorkflow(ctx context.Context, workflowID, nodeID string) (*models.WorkflowNode, error) {
	return nil, errors.New("mock node repository not implemented during transition")
}

func (nr *MockNodeRepository) SaveNode(ctx context.Context, workflowID string, node *models.WorkflowNode) error {
	return errors.New("mock node repository not implemented during transition")
}

func (nr *MockNodeRepository) UpdateNode(ctx context.Context, workflowID string, node *models.WorkflowNode) error {
	return errors.New("mock node repository not implemented during transition")
}

func (nr *MockNodeRepository) DeleteNode(ctx context.Context, workflowID, nodeID string) error {
	return errors.New("mock node repository not implemented during transition")
}

func (nr *MockNodeRepository) DeleteNodeWithConnections(ctx context.Context, workflowID, nodeID string) error {
	args := nr.Called(ctx, workflowID, nodeID)

	return args.Error(0)
}

func (nr *MockNodeRepository) FindTriggerNodesBySourceEventAndProvider(ctx context.Context, sourceID, eventType, providerID string, status models.WorkflowStatus) ([]*models.TriggerNodeMatch, error) {
	args := nr.Called(ctx, sourceID, eventType, providerID, status)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*models.TriggerNodeMatch), args.Error(1)
}

type MockConnectionRepository struct{}

func (cr *MockConnectionRepository) GetConnectionsBySourceNode(ctx context.Context, workflowID, sourceNodeID string) ([]*models.Connection, error) {
	return nil, errors.New("mock connection repository not implemented during transition")
}

func (cr *MockConnectionRepository) GetConnectionsByTargetNode(ctx context.Context, workflowID, targetNodeID string) ([]*models.Connection, error) {
	return nil, errors.New("mock connection repository not implemented during transition")
}

func (cr *MockConnectionRepository) SaveConnection(ctx context.Context, workflowID string, connection *models.Connection) error {
	return errors.New("mock connection repository not implemented during transition")
}

func (cr *MockConnectionRepository) UpdateConnection(ctx context.Context, workflowID string, connection *models.Connection) error {
	return errors.New("mock connection repository not implemented during transition")
}

func (cr *MockConnectionRepository) DeleteConnection(ctx context.Context, workflowID, connectionID string) error {
	return errors.New("mock connection repository not implemented during transition")
}

func (cr *MockConnectionRepository) GetConnectionsByWorkflow(ctx context.Context, workflowID string) ([]*models.Connection, error) {
	return nil, errors.New("mock connection repository not implemented during transition")
}

type MockExecutionContextRepository struct {
	mock.Mock
}

func (ecr *MockExecutionContextRepository) SaveExecutionContext(ctx context.Context, execCtx *models.ExecutionContext) error {
	args := ecr.Called(ctx, execCtx)

	return args.Error(0)
}

func (ecr *MockExecutionContextRepository) GetExecutionContext(ctx context.Context, executionID string) (*models.ExecutionContext, error) {
	args := ecr.Called(ctx, executionID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.ExecutionContext), args.Error(1)
}

func (ecr *MockExecutionContextRepository) UpdateExecutionContext(ctx context.Context, execCtx *models.ExecutionContext) error {
	args := ecr.Called(ctx, execCtx)

	return args.Error(0)
}

func (ecr *MockExecutionContextRepository) GetExecutionsByWorkflow(ctx context.Context, workflowID string) ([]*models.ExecutionContext, error) {
	args := ecr.Called(ctx, workflowID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*models.ExecutionContext), args.Error(1)
}

func (ecr *MockExecutionContextRepository) GetExecutionsByStatus(ctx context.Context, status models.ExecutionStatus) ([]*models.ExecutionContext, error) {
	args := ecr.Called(ctx, status)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*models.ExecutionContext), args.Error(1)
}

type MockInputCoordinationRepository struct{}

func (icr *MockInputCoordinationRepository) SaveInputState(ctx context.Context, state *models.NodeInputState) error {
	return errors.New("mock input coordination repository not implemented during transition")
}

func (icr *MockInputCoordinationRepository) LoadInputState(ctx context.Context, nodeExecutionID string) (*models.NodeInputState, error) {
	return nil, errors.New("mock input coordination repository not implemented during transition")
}

func (icr *MockInputCoordinationRepository) FindPendingNodeExecution(ctx context.Context, nodeID, executionID string) (*models.NodeInputState, error) {
	return nil, errors.New("mock input coordination repository not implemented during transition")
}

func (icr *MockInputCoordinationRepository) DeleteInputState(ctx context.Context, nodeExecutionID string) error {
	return errors.New("mock input coordination repository not implemented during transition")
}

func (icr *MockInputCoordinationRepository) CleanupExpiredStates(ctx context.Context, maxAge time.Duration) error {
	return errors.New("mock input coordination repository not implemented during transition")
}
