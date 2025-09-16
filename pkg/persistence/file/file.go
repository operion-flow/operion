// Package file provides file-based persistence implementation for workflows and triggers.
package file

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
)

// Persistence implements the persistence.Persistence interface using the file system.
type Persistence struct {
	root                 string
	workflowRepo         *WorkflowRepository
	executionContextRepo *ExecutionContextRepository
}

// NewPersistence creates a new instance of Persistence with the specified root directory.
func NewPersistence(root string) persistence.Persistence {
	cleanRoot := strings.Replace(root, "file://", "", 1)

	return &Persistence{
		root:                 cleanRoot,
		workflowRepo:         NewWorkflowRepository(cleanRoot),
		executionContextRepo: NewExecutionContextRepository(cleanRoot),
	}
}

// Close performs any necessary cleanup. For file-based persistence, there is nothing to clean up.
func (fp *Persistence) Close(_ context.Context) error {
	return nil
}

// HealthCheck checks if the file persistence layer is healthy by verifying the root directory exists.
func (fp *Persistence) HealthCheck(_ context.Context) error {
	if _, err := os.Stat(fp.root); os.IsNotExist(err) {
		return os.ErrNotExist
	}

	return nil
}

// WorkflowRepository returns the workflow repository implementation for file persistence.
func (fp *Persistence) WorkflowRepository() persistence.WorkflowRepository {
	return fp.workflowRepo
}

// Node-based repository interface implementations (not implemented for file persistence)
// These return not implemented errors as file persistence doesn't support the node-based architecture

func (fp *Persistence) NodeRepository() persistence.NodeRepository {
	return &nodeRepository{persistence: fp}
}

func (fp *Persistence) ConnectionRepository() persistence.ConnectionRepository {
	return &connectionRepository{persistence: fp}
}

func (fp *Persistence) ExecutionContextRepository() persistence.ExecutionContextRepository {
	return fp.executionContextRepo
}

func (fp *Persistence) InputCoordinationRepository() persistence.InputCoordinationRepository {
	return NewFileInputCoordinationRepository(fp.root)
}

// Node repository implementation for file persistence
// This works by reading workflow files and extracting node information

type nodeRepository struct {
	persistence *Persistence
}

func (nr *nodeRepository) GetNodesByWorkflow(ctx context.Context, workflowID string) ([]*models.WorkflowNode, error) {
	workflow, err := nr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	return workflow.Nodes, nil
}

func (nr *nodeRepository) GetNodeByWorkflow(ctx context.Context, workflowID, nodeID string) (*models.WorkflowNode, error) {
	workflow, err := nr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	// Find the specific node
	for _, node := range workflow.Nodes {
		if node.ID == nodeID {
			return node, nil
		}
	}

	return nil, fmt.Errorf("node not found: %s in workflow %s", nodeID, workflowID)
}

func (nr *nodeRepository) SaveNode(ctx context.Context, workflowID string, node *models.WorkflowNode) error {
	workflow, err := nr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return fmt.Errorf("workflow not found: %s", workflowID)
	}

	// Check if node already exists
	for i, existingNode := range workflow.Nodes {
		if existingNode.ID == node.ID {
			// Update existing node
			workflow.Nodes[i] = node

			return nr.persistence.workflowRepo.Save(ctx, workflow)
		}
	}

	// Add new node
	workflow.Nodes = append(workflow.Nodes, node)

	return nr.persistence.workflowRepo.Save(ctx, workflow)
}

func (nr *nodeRepository) UpdateNode(ctx context.Context, workflowID string, node *models.WorkflowNode) error {
	return nr.SaveNode(ctx, workflowID, node)
}

func (nr *nodeRepository) DeleteNode(ctx context.Context, workflowID, nodeID string) error {
	workflow, err := nr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return fmt.Errorf("workflow not found: %s", workflowID)
	}

	// Find and remove the node
	for i, node := range workflow.Nodes {
		if node.ID == nodeID {
			workflow.Nodes = append(workflow.Nodes[:i], workflow.Nodes[i+1:]...)

			return nr.persistence.workflowRepo.Save(ctx, workflow)
		}
	}

	return fmt.Errorf("node not found: %s in workflow %s", nodeID, workflowID)
}

func (nr *nodeRepository) FindTriggerNodesBySourceEventAndProvider(ctx context.Context, sourceID, eventType, providerID string, status models.WorkflowStatus) ([]*models.TriggerNodeMatch, error) {
	// Get all workflows with the status filter
	result, err := nr.persistence.workflowRepo.ListWorkflows(ctx, persistence.ListWorkflowsOptions{
		Status: &status,
		Limit:  100, // Get all workflows for trigger matching
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get workflows: %w", err)
	}

	var matches []*models.TriggerNodeMatch

	for _, workflow := range result.Workflows {
		// Check each node for trigger nodes that match
		for _, node := range workflow.Nodes {
			// Check if this is a trigger node matching our criteria
			if node.Category == models.CategoryTypeTrigger &&
				node.SourceID != nil && *node.SourceID == sourceID &&
				node.EventType != nil && *node.EventType == eventType &&
				node.ProviderID != nil && *node.ProviderID == providerID &&
				node.Enabled {
				match := &models.TriggerNodeMatch{
					WorkflowID:  workflow.ID,
					TriggerNode: node,
				}
				matches = append(matches, match)
			}
		}
	}

	return matches, nil
}

type connectionRepository struct {
	persistence *Persistence
}

func (cr *connectionRepository) GetConnectionsBySourceNode(ctx context.Context, workflowID, sourceNodeID string) ([]*models.Connection, error) {
	workflow, err := cr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	var connections []*models.Connection
	for _, conn := range workflow.Connections {
		// Parse source port to get node ID
		nodeID, _, ok := models.ParsePortID(conn.SourcePort)
		if ok && nodeID == sourceNodeID {
			connections = append(connections, conn)
		}
	}

	return connections, nil
}

func (cr *connectionRepository) GetConnectionsByTargetNode(ctx context.Context, workflowID, targetNodeID string) ([]*models.Connection, error) {
	workflow, err := cr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	var connections []*models.Connection
	for _, conn := range workflow.Connections {
		// Parse target port to get node ID
		nodeID, _, ok := models.ParsePortID(conn.TargetPort)
		if ok && nodeID == targetNodeID {
			connections = append(connections, conn)
		}
	}

	return connections, nil
}

func (cr *connectionRepository) SaveConnection(ctx context.Context, workflowID string, connection *models.Connection) error {
	workflow, err := cr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return fmt.Errorf("workflow not found: %s", workflowID)
	}

	// Check if connection already exists
	for i, existingConn := range workflow.Connections {
		if existingConn.ID == connection.ID {
			// Update existing connection
			workflow.Connections[i] = connection

			return cr.persistence.workflowRepo.Save(ctx, workflow)
		}
	}

	// Add new connection
	workflow.Connections = append(workflow.Connections, connection)

	return cr.persistence.workflowRepo.Save(ctx, workflow)
}

func (cr *connectionRepository) UpdateConnection(ctx context.Context, workflowID string, connection *models.Connection) error {
	return cr.SaveConnection(ctx, workflowID, connection)
}

func (cr *connectionRepository) DeleteConnection(ctx context.Context, workflowID, connectionID string) error {
	workflow, err := cr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return fmt.Errorf("workflow not found: %s", workflowID)
	}

	// Find and remove the connection
	for i, conn := range workflow.Connections {
		if conn.ID == connectionID {
			workflow.Connections = append(workflow.Connections[:i], workflow.Connections[i+1:]...)

			return cr.persistence.workflowRepo.Save(ctx, workflow)
		}
	}

	return fmt.Errorf("connection not found: %s in workflow %s", connectionID, workflowID)
}

func (cr *connectionRepository) GetConnectionsByWorkflow(ctx context.Context, workflowID string) ([]*models.Connection, error) {
	workflow, err := cr.persistence.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow %s: %w", workflowID, err)
	}

	if workflow == nil {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	return workflow.Connections, nil
}
