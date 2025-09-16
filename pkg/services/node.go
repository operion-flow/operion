// Package services provides node management functionality for workflows.
package services

import (
	"context"
	"fmt"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/google/uuid"
)

// validateWorkflowForNodeModification checks if workflow allows node modifications.
func validateWorkflowForNodeModification(workflow *models.Workflow) error {
	if workflow.Status != models.WorkflowStatusDraft {
		switch workflow.Status {
		case models.WorkflowStatusDraft:
			// This case should never be reached due to the outer condition
			return nil
		case models.WorkflowStatusPublished:
			return ErrCannotModifyPublished
		case models.WorkflowStatusUnpublished:
			return ErrCannotModifyUnpublished
		default:
			return fmt.Errorf("cannot modify nodes in %s workflow", workflow.Status)
		}
	}

	return nil
}

// CreateNodeRequest represents the request to create a new workflow node.
type CreateNodeRequest struct {
	Type      string
	Category  string
	Config    map[string]any
	PositionX int
	PositionY int
	Name      string
	Enabled   bool
}

// UpdateNodeRequest represents the request to update an existing workflow node.
type UpdateNodeRequest struct {
	Config    map[string]any
	PositionX int
	PositionY int
	Name      string
	Enabled   bool
}

// Node handles node-related business operations.
type Node struct {
	persistence persistence.Persistence
}

// NewNode creates a new node service.
func NewNode(persistence persistence.Persistence) *Node {
	return &Node{
		persistence: persistence,
	}
}

// CreateNode creates a new node in the specified workflow.
func (n *Node) CreateNode(ctx context.Context, workflowID string, req *CreateNodeRequest) (*models.WorkflowNode, error) {
	// Validate workflow exists and is in draft status
	workflow, err := n.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		if persistence.IsWorkflowNotFound(err) {
			return nil, persistence.ErrWorkflowNotFound
		}

		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	// Handle case where workflow doesn't exist (nil return)
	if workflow == nil {
		return nil, persistence.ErrWorkflowNotFound
	}

	// Only allow modifications on draft workflows
	if err := validateWorkflowForNodeModification(workflow); err != nil {
		return nil, err
	}

	// Create new node with generated UUID
	node := &models.WorkflowNode{
		ID:        uuid.New().String(),
		Type:      req.Type,
		Category:  models.CategoryType(req.Category),
		Name:      req.Name,
		Config:    req.Config,
		PositionX: req.PositionX,
		PositionY: req.PositionY,
		Enabled:   req.Enabled,
	}

	// Initialize config if nil
	if node.Config == nil {
		node.Config = make(map[string]any)
	}

	// Save the node
	err = n.persistence.NodeRepository().SaveNode(ctx, workflowID, node)
	if err != nil {
		// Map persistence errors to service errors
		if persistence.IsNodeNotFound(err) {
			return nil, ErrNodeNotFound
		}

		return nil, fmt.Errorf("failed to save node: %w", err)
	}

	return node, nil
}

// GetNode retrieves a specific node from the specified workflow.
func (n *Node) GetNode(ctx context.Context, workflowID, nodeID string) (*models.WorkflowNode, error) {
	node, err := n.persistence.NodeRepository().GetNodeByWorkflow(ctx, workflowID, nodeID)
	if err != nil {
		// Map persistence errors to service errors
		if persistence.IsNodeNotFound(err) {
			return nil, ErrNodeNotFound
		}

		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	return node, nil
}

// UpdateNode updates an existing node in the specified workflow.
func (n *Node) UpdateNode(ctx context.Context, workflowID, nodeID string, req *UpdateNodeRequest) (*models.WorkflowNode, error) {
	// Validate workflow exists and is in draft status
	workflow, err := n.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		if persistence.IsWorkflowNotFound(err) {
			return nil, persistence.ErrWorkflowNotFound
		}

		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	// Handle case where workflow doesn't exist (nil return)
	if workflow == nil {
		return nil, persistence.ErrWorkflowNotFound
	}

	// Only allow modifications on draft workflows
	if err := validateWorkflowForNodeModification(workflow); err != nil {
		return nil, err
	}

	// Get existing node
	existingNode, err := n.persistence.NodeRepository().GetNodeByWorkflow(ctx, workflowID, nodeID)
	if err != nil {
		// Map persistence errors to service errors
		if persistence.IsNodeNotFound(err) {
			return nil, ErrNodeNotFound
		}

		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	// Update only the allowed fields (preserve Type and Category)
	existingNode.Name = req.Name
	existingNode.Config = req.Config
	existingNode.PositionX = req.PositionX
	existingNode.PositionY = req.PositionY
	existingNode.Enabled = req.Enabled

	// Initialize config if nil
	if existingNode.Config == nil {
		existingNode.Config = make(map[string]any)
	}

	// Update the node
	err = n.persistence.NodeRepository().UpdateNode(ctx, workflowID, existingNode)
	if err != nil {
		// Map persistence errors to service errors
		if persistence.IsNodeNotFound(err) {
			return nil, ErrNodeNotFound
		}

		return nil, fmt.Errorf("failed to update node: %w", err)
	}

	return existingNode, nil
}

// DeleteNode deletes a node and all its associated connections from the specified workflow.
func (n *Node) DeleteNode(ctx context.Context, workflowID, nodeID string) error {
	// Validate workflow exists and is in draft status
	workflow, err := n.persistence.WorkflowRepository().GetByID(ctx, workflowID)
	if err != nil {
		if persistence.IsWorkflowNotFound(err) {
			return persistence.ErrWorkflowNotFound
		}

		return fmt.Errorf("failed to get workflow: %w", err)
	}

	// Handle case where workflow doesn't exist (nil return)
	if workflow == nil {
		return persistence.ErrWorkflowNotFound
	}

	// Only allow modifications on draft workflows
	if err := validateWorkflowForNodeModification(workflow); err != nil {
		return err
	}

	// Delete the node and its connections using transaction
	err = n.persistence.NodeRepository().DeleteNodeWithConnections(ctx, workflowID, nodeID)
	if err != nil {
		// Map persistence errors to service errors
		if persistence.IsNodeNotFound(err) {
			return ErrNodeNotFound
		}

		return fmt.Errorf("failed to delete node: %w", err)
	}

	return nil
}
