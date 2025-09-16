// Package web provides HTTP handlers and REST API endpoints for workflow management.
package web

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dukex/operion/pkg/eventbus"
	"github.com/dukex/operion/pkg/events"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/services"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
)

type APIHandlers struct {
	workflowService   *services.Workflow
	publishingService *services.Publishing
	nodeService       *services.Node
	validator         *validator.Validate
	registry          *registry.Registry
	eventBus          eventbus.EventBus
}

func NewAPIHandlers(
	workflowService *services.Workflow,
	publishingService *services.Publishing,
	nodeService *services.Node,
	validator *validator.Validate,
	registry *registry.Registry,
	eventBus eventbus.EventBus,
) *APIHandlers {
	return &APIHandlers{
		workflowService:   workflowService,
		publishingService: publishingService,
		nodeService:       nodeService,
		validator:         validator,
		registry:          registry,
		eventBus:          eventBus,
	}
}

func (h *APIHandlers) GetWorkflows(c fiber.Ctx) error {
	// Parse query parameters
	req, err := h.parseListWorkflowsRequest(c)
	if err != nil {
		return badRequest(c, "Invalid query parameters: "+err.Error())
	}

	// Call service layer
	result, err := h.workflowService.ListWorkflows(c.Context(), req)
	if err != nil {
		return handleServiceError(c, err)
	}

	// Return structured response with pagination metadata
	return c.JSON(fiber.Map{
		"workflows":     result.Workflows,
		"total_count":   result.TotalCount,
		"has_next_page": result.HasNextPage,
		"pagination": fiber.Map{
			"page":     req.Page,
			"per_page": req.PerPage,
		},
		"sorting": fiber.Map{
			"sort_by":    req.SortBy,
			"sort_order": req.SortOrder,
		},
	})
}

// parseListWorkflowsRequest parses and validates query parameters for listing workflows.
func (h *APIHandlers) parseListWorkflowsRequest(c fiber.Ctx) (*services.ListWorkflowsRequest, error) {
	req := &services.ListWorkflowsRequest{}

	// Parse pagination parameters
	if pageStr := c.Query("page"); pageStr != "" {
		page, err := strconv.Atoi(pageStr)
		if err != nil {
			return nil, err
		}

		req.Page = page
	}

	if perPageStr := c.Query("per_page"); perPageStr != "" {
		perPage, err := strconv.Atoi(perPageStr)
		if err != nil {
			return nil, err
		}

		req.PerPage = perPage
	}

	// Parse filtering parameters
	req.OwnerID = c.Query("owner_id")

	if statusStr := c.Query("status"); statusStr != "" {
		status := models.WorkflowStatus(statusStr)
		req.Status = &status
	}

	// Parse sorting parameters
	req.SortBy = c.Query("sort_by")
	req.SortOrder = c.Query("sort_order")

	// Parse data loading parameters
	if includeNodesStr := c.Query("include_nodes"); includeNodesStr != "" {
		includeNodes, err := strconv.ParseBool(includeNodesStr)
		if err != nil {
			return nil, err
		}

		req.IncludeNodes = includeNodes
	}

	if includeConnectionsStr := c.Query("include_connections"); includeConnectionsStr != "" {
		includeConnections, err := strconv.ParseBool(includeConnectionsStr)
		if err != nil {
			return nil, err
		}

		req.IncludeConnections = includeConnections
	}

	return req, nil
}

func (h *APIHandlers) GetWorkflow(c fiber.Ctx) error {
	id := c.Params("id")

	if id == "" {
		return badRequest(c, "Workflow ID is required")
	}

	workflow, err := h.workflowService.FetchByID(c.Context(), id)
	if err != nil {
		return handleServiceError(c, err)
	}

	return c.JSON(workflow)
}

func (h *APIHandlers) HealthCheck(c fiber.Ctx) error {
	registryCheck, regOk := h.registry.HealthCheck()
	repositoryCheck, repOk := h.workflowService.HealthCheck(c.Context())

	status := "unhealthy"
	message := "Operion API is unhealthy"
	httpStatus := http.StatusInternalServerError

	if regOk && repOk {
		status = "healthy"
		message = "Operion API is healthy"
		httpStatus = http.StatusOK
	}

	return c.Status(httpStatus).JSON(fiber.Map{
		"status":  status,
		"message": message,
		"checkers": fiber.Map{
			"registry":   registryCheck,
			"repository": repositoryCheck,
		},
		"timestamp": time.Now().UTC(),
	})
}

func (h *APIHandlers) CreateWorkflow(c fiber.Ctx) error {
	var req CreateWorkflowRequest
	if err := c.Bind().JSON(&req); err != nil {
		return badRequest(c, "Invalid JSON format")
	}

	if err := h.validator.Struct(req); err != nil {
		return badRequest(c, err.Error())
	}

	workflow := &models.Workflow{
		Name:        req.Name,
		Description: req.Description,
		Variables:   req.Variables,
		Metadata:    req.Metadata,
		Owner:       req.Owner,
		Nodes:       []*models.WorkflowNode{}, // Empty - nodes added separately
		Connections: []*models.Connection{},   // Empty - connections added separately
	}

	created, err := h.workflowService.Create(c.Context(), workflow)
	if err != nil {
		return handleServiceError(c, err)
	}

	return c.Status(fiber.StatusCreated).JSON(created)
}

func (h *APIHandlers) UpdateWorkflow(c fiber.Ctx) error {
	id := c.Params("id")
	if id == "" {
		return badRequest(c, "Workflow ID is required")
	}

	var req UpdateWorkflowRequest
	if err := c.Bind().JSON(&req); err != nil {
		return badRequest(c, "Invalid JSON format")
	}

	if err := h.validator.Struct(req); err != nil {
		return badRequest(c, err.Error())
	}

	// Get existing workflow and merge changes
	existing, err := h.workflowService.FetchByID(c.Context(), id)
	if err != nil {
		return handleServiceError(c, err)
	}

	// Apply partial updates (nodes and connections managed separately)
	if req.Name != nil {
		existing.Name = *req.Name
	}

	if req.Description != nil {
		existing.Description = *req.Description
	}

	if req.Variables != nil {
		existing.Variables = req.Variables
	}

	if req.Metadata != nil {
		existing.Metadata = req.Metadata
	}

	updated, err := h.workflowService.Update(c.Context(), existing)
	if err != nil {
		return handleServiceError(c, err)
	}

	return c.JSON(updated)
}

func (h *APIHandlers) DeleteWorkflow(c fiber.Ctx) error {
	id := c.Params("id")
	if id == "" {
		return badRequest(c, "Workflow ID is required")
	}

	err := h.workflowService.Delete(c.Context(), id)
	if err != nil {
		return handleServiceError(c, err)
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func (h *APIHandlers) PublishWorkflow(c fiber.Ctx) error {
	id := c.Params("id")
	if id == "" {
		return badRequest(c, "Workflow ID is required")
	}

	published, err := h.publishingService.PublishWorkflow(c.Context(), id)
	if err != nil {
		return handleServiceError(c, err)
	}

	// Publish domain event for workflow publishing with trigger nodes
	triggerNodes := h.extractTriggerNodes(published)
	if len(triggerNodes) > 0 {
		event := events.NewWorkflowPublishedEvent(
			published.ID,
			published.Name,
			triggerNodes,
			h.getUserID(c),
		)

		// Async event publishing - API doesn't wait for source configuration
		if err := h.eventBus.Publish(c.Context(), "operion.workflow.published", event); err != nil {
			h.logEventPublishError("workflow.published", published.ID, err)
		}
	}

	return c.JSON(published)
}

// extractTriggerNodes extracts trigger nodes from a published workflow for event publishing.
func (h *APIHandlers) extractTriggerNodes(workflow *models.Workflow) []events.TriggerNode {
	triggerNodes := make([]events.TriggerNode, 0)

	if workflow.Nodes == nil {
		return triggerNodes
	}

	for _, node := range workflow.Nodes {
		if h.isTriggerNode(node.Type) {
			sourceID := ""
			if node.SourceID != nil {
				sourceID = *node.SourceID
			}

			triggerNode := events.TriggerNode{
				ID:       node.ID,
				Type:     node.Type,
				Config:   node.Config,
				SourceID: sourceID,
			}
			triggerNodes = append(triggerNodes, triggerNode)
		}
	}

	return triggerNodes
}

func (h *APIHandlers) CreateDraftFromPublished(c fiber.Ctx) error {
	groupID := c.Params("groupId")
	if groupID == "" {
		return badRequest(c, "Workflow group ID is required")
	}

	draft, err := h.publishingService.CreateDraftFromPublished(c.Context(), groupID)
	if err != nil {
		if persistence.IsPublishedWorkflowNotFound(err) {
			return notFound(c, "Published workflow not found")
		}

		return internalError(c, err)
	}

	return c.Status(fiber.StatusCreated).JSON(draft)
}

// CreateWorkflowNode creates a new node in the specified workflow.
func (h *APIHandlers) CreateWorkflowNode(c fiber.Ctx) error {
	workflowID := c.Params("id")
	if workflowID == "" {
		return badRequest(c, "Workflow ID is required")
	}

	var req CreateNodeRequest
	if err := c.Bind().JSON(&req); err != nil {
		return badRequest(c, "Invalid JSON format")
	}

	if err := h.validator.Struct(req); err != nil {
		return badRequest(c, err.Error())
	}

	// Convert web request to service request
	serviceReq := &services.CreateNodeRequest{
		Type:      req.Type,
		Category:  req.Category,
		Config:    req.Config,
		PositionX: req.PositionX,
		PositionY: req.PositionY,
		Name:      req.Name,
		Enabled:   req.Enabled,
	}

	node, err := h.nodeService.CreateNode(c.Context(), workflowID, serviceReq)
	if err != nil {
		return handleServiceError(c, err)
	}

	// Publish domain event for trigger node creation
	if h.isTriggerNode(node.Type) {
		event := events.NewTriggerCreatedEvent(
			node.ID,
			workflowID,
			node.Type,
			node.Config,
			h.getUserID(c),
		)

		// Async event publishing - API doesn't wait for source configuration
		if err := h.eventBus.Publish(c.Context(), "operion.trigger.created", event); err != nil {
			// Log error but don't fail the request - event publishing is non-critical
			// Source manager can recover from missed events via eventual consistency
			h.logEventPublishError("trigger.created", node.ID, err)
		}
	}

	return c.Status(fiber.StatusCreated).JSON(node)
}

// isTriggerNode checks if a node type represents a trigger node.
func (h *APIHandlers) isTriggerNode(nodeType string) bool {
	return strings.HasPrefix(nodeType, "trigger:")
}

// getUserID extracts the user ID from the fiber context
// This would typically come from authentication middleware.
func (h *APIHandlers) getUserID(c fiber.Ctx) string {
	// TODO: Extract from auth context when authentication is implemented
	// For now, return empty string or a default value
	if userID := c.Get("X-User-ID"); userID != "" {
		return userID
	}

	return "system" // Default fallback
}

// logEventPublishError logs event publishing errors without failing the request.
func (h *APIHandlers) logEventPublishError(eventType, entityID string, err error) {
	// TODO: Use proper structured logger when available
	// For now, this is a placeholder for logging infrastructure
	_ = eventType
	_ = entityID
	_ = err
	// log.Error("Failed to publish event", "event_type", eventType, "entity_id", entityID, "error", err)
}

// UpdateWorkflowNode updates an existing node in the specified workflow.
func (h *APIHandlers) UpdateWorkflowNode(c fiber.Ctx) error {
	workflowID := c.Params("id")
	if workflowID == "" {
		return badRequest(c, "Workflow ID is required")
	}

	nodeID := c.Params("nodeId")
	if nodeID == "" {
		return badRequest(c, "Node ID is required")
	}

	var req UpdateNodeRequest
	if err := c.Bind().JSON(&req); err != nil {
		return badRequest(c, "Invalid JSON format")
	}

	if err := h.validator.Struct(req); err != nil {
		return badRequest(c, err.Error())
	}

	// Convert web request to service request
	serviceReq := &services.UpdateNodeRequest{
		Config:    req.Config,
		PositionX: req.PositionX,
		PositionY: req.PositionY,
		Name:      req.Name,
		Enabled:   req.Enabled,
	}

	// Get the existing node before update to capture previous config for event
	existingNode, err := h.nodeService.GetNode(c.Context(), workflowID, nodeID)
	if err != nil {
		return handleServiceError(c, err)
	}

	node, err := h.nodeService.UpdateNode(c.Context(), workflowID, nodeID, serviceReq)
	if err != nil {
		return handleServiceError(c, err)
	}

	// Publish domain event for trigger node updates
	if h.isTriggerNode(node.Type) {
		event := events.NewTriggerUpdatedEvent(
			node.ID,
			workflowID,
			node.Type,
			node.Config,
			existingNode.Config, // Previous configuration for comparison
			h.getUserID(c),
		)

		// Async event publishing - API doesn't wait for source configuration
		if err := h.eventBus.Publish(c.Context(), "operion.trigger.updated", event); err != nil {
			h.logEventPublishError("trigger.updated", node.ID, err)
		}
	}

	return c.JSON(node)
}

// DeleteWorkflowNode deletes a node and all its associated connections from the specified workflow.
func (h *APIHandlers) DeleteWorkflowNode(c fiber.Ctx) error {
	workflowID := c.Params("id")
	if workflowID == "" {
		return badRequest(c, "Workflow ID is required")
	}

	nodeID := c.Params("nodeId")
	if nodeID == "" {
		return badRequest(c, "Node ID is required")
	}

	// Get the node before deletion for event publishing
	node, err := h.nodeService.GetNode(c.Context(), workflowID, nodeID)
	if err != nil {
		return handleServiceError(c, err)
	}

	err = h.nodeService.DeleteNode(c.Context(), workflowID, nodeID)
	if err != nil {
		return handleServiceError(c, err)
	}

	// Publish domain event for trigger node deletion
	if h.isTriggerNode(node.Type) {
		sourceID := ""
		if node.SourceID != nil {
			sourceID = *node.SourceID
		}

		event := events.NewTriggerDeletedEvent(
			node.ID,
			workflowID,
			node.Type,
			sourceID,
			h.getUserID(c),
		)

		// Async event publishing - API doesn't wait for source cleanup
		if err := h.eventBus.Publish(c.Context(), "operion.trigger.deleted", event); err != nil {
			h.logEventPublishError("trigger.deleted", node.ID, err)
		}
	}

	return c.SendStatus(fiber.StatusNoContent)
}

// GetWorkflowNode retrieves a specific node from a workflow.
func (h *APIHandlers) GetWorkflowNode(c fiber.Ctx) error {
	workflowID := c.Params("id")
	if workflowID == "" {
		return badRequest(c, "Workflow ID is required")
	}

	nodeID := c.Params("nodeId")
	if nodeID == "" {
		return badRequest(c, "Node ID is required")
	}

	node, err := h.nodeService.GetNode(c.Context(), workflowID, nodeID)
	if err != nil {
		return handleServiceError(c, err)
	}

	// Transform response based on node type
	response := TransformNodeResponse(node)

	return c.JSON(response)
}
