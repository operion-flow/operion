package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/remote"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/moogar0880/problems"
)

const (
	defaultExecutionsLimit = 50
	maxExecutionsLimit     = 500
	tenantHeader           = "X-Tenant-Id"
)

var errTenantRequired = errors.New(tenantHeader + " header is required")

type Workflows interface {
	FetchAll(ctx context.Context, tenantID string) ([]*models.Workflow, error)
	FetchByID(ctx context.Context, id string) (*models.Workflow, error)
	Create(ctx context.Context, workflow *models.Workflow) (*models.Workflow, error)
}

type Publisher interface {
	PublishWorkflow(ctx context.Context, workflowID string) (*models.Workflow, error)
}

type Executions interface {
	GetExecutionContext(ctx context.Context, executionID string) (*models.ExecutionContext, error)
	GetExecutionsByWorkflow(ctx context.Context, workflowID string, limit int) ([]*models.ExecutionContext, error)
}

type Catalog interface {
	Save(ctx context.Context, definition *models.EventDefinition) error
	Get(ctx context.Context, key string) (*models.EventDefinition, error)
	List(ctx context.Context) ([]*models.EventDefinition, error)
}

type NodeTypes interface {
	Save(ctx context.Context, nodeType *models.RemoteNodeType) error
	Get(ctx context.Context, id string) (*models.RemoteNodeType, error)
}

type API struct {
	workflows  Workflows
	publisher  Publisher
	executions Executions
	catalog    Catalog
	nodeTypes  NodeTypes
	validate   *validator.Validate
}

func New(workflows Workflows, publisher Publisher, executions Executions, catalog Catalog, nodeTypes NodeTypes) *API {
	return &API{
		workflows:  workflows,
		publisher:  publisher,
		executions: executions,
		catalog:    catalog,
		nodeTypes:  nodeTypes,
		validate:   validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (a *API) Register(app *fiber.App) {
	app.Get("/workflows", a.listWorkflows)
	app.Post("/workflows", a.createWorkflow)
	app.Get("/workflows/:id", a.getWorkflow)
	app.Post("/workflows/:id/publish", a.publishWorkflow)
	app.Get("/workflows/:id/executions", a.listExecutions)
	app.Get("/executions/:id", a.getExecution)
	app.Put("/platform/event-definitions/:key", a.saveEventDefinition)
	app.Get("/platform/event-definitions/:key", a.getEventDefinition)
	app.Get("/catalog/events", a.listCatalog)
	app.Put("/platform/node-types/:id", a.saveNodeType)
	app.Get("/platform/node-types/:id", a.getNodeType)
}

func (a *API) listWorkflows(c fiber.Ctx) error {
	tenant, err := tenantOf(c)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	workflows, err := a.workflows.FetchAll(c.Context(), tenant)
	if err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	return c.JSON(workflows)
}

func (a *API) createWorkflow(c fiber.Ctx) error {
	tenant, err := tenantOf(c)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	var wf models.Workflow
	if err := json.Unmarshal(c.Body(), &wf); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	wf.Status = models.WorkflowStatusDraft
	wf.TenantID = tenant

	if err := a.validate.Struct(&wf); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	created, err := a.workflows.Create(c.Context(), &wf)
	if err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	return c.Status(fiber.StatusCreated).JSON(created)
}

func (a *API) getWorkflow(c fiber.Ctx) error {
	wf, err := a.ownedWorkflow(c, c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(wf)
}

func (a *API) publishWorkflow(c fiber.Ctx) error {
	wf, err := a.ownedWorkflow(c, c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	published, err := a.publisher.PublishWorkflow(c.Context(), wf.ID)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(published)
}

func (a *API) listExecutions(c fiber.Ctx) error {
	wf, err := a.ownedWorkflow(c, c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	limit := defaultExecutionsLimit

	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxExecutionsLimit {
			return problem(c, fiber.StatusBadRequest, fmt.Errorf("limit must be a number between 1 and %d", maxExecutionsLimit))
		}

		limit = parsed
	}

	executions, err := a.executions.GetExecutionsByWorkflow(c.Context(), wf.ID, limit)
	if err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	if executions == nil {
		executions = []*models.ExecutionContext{}
	}

	return c.JSON(executions)
}

func (a *API) getExecution(c fiber.Ctx) error {
	execution, err := a.executions.GetExecutionContext(c.Context(), c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	if _, err := a.ownedWorkflow(c, execution.WorkflowID); err != nil {
		if errors.Is(err, workflow.ErrWorkflowNotFound) {
			err = fmt.Errorf("%w: %s", persistence.ErrExecutionNotFound, execution.ID)
		}

		return problem(c, statusFor(err), err)
	}

	return c.JSON(execution)
}

func tenantOf(c fiber.Ctx) (string, error) {
	tenant := c.Get(tenantHeader)
	if tenant == "" {
		return "", errTenantRequired
	}

	return tenant, nil
}

func (a *API) ownedWorkflow(c fiber.Ctx, id string) (*models.Workflow, error) {
	tenant, err := tenantOf(c)
	if err != nil {
		return nil, err
	}

	wf, err := a.workflows.FetchByID(c.Context(), id)
	if err != nil {
		return nil, err
	}

	if wf.TenantID != tenant {
		return nil, fmt.Errorf("%w: %s", workflow.ErrWorkflowNotFound, id)
	}

	return wf, nil
}

func (a *API) saveEventDefinition(c fiber.Ctx) error {
	var definition models.EventDefinition
	if err := json.Unmarshal(c.Body(), &definition); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	definition.Key = c.Params("key")

	if err := a.validate.Struct(&definition); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	if err := a.catalog.Save(c.Context(), &definition); err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	return c.JSON(definition)
}

func (a *API) getEventDefinition(c fiber.Ctx) error {
	definition, err := a.catalog.Get(c.Context(), c.Params("key"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(definition)
}

type catalogEvent struct {
	Key         string         `json:"key"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Schema      map[string]any `json:"schema,omitempty"`
}

func (a *API) listCatalog(c fiber.Ctx) error {
	definitions, err := a.catalog.List(c.Context())
	if err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	events := make([]catalogEvent, 0, len(definitions))
	for _, definition := range definitions {
		events = append(events, catalogEvent{
			Key:         definition.Key,
			Name:        definition.Name,
			Description: definition.Description,
			Schema:      definition.Schema,
		})
	}

	return c.JSON(events)
}

func (a *API) saveNodeType(c fiber.Ctx) error {
	var nodeType models.RemoteNodeType
	if err := json.Unmarshal(c.Body(), &nodeType); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	nodeType.ID = c.Params("id")

	if err := a.validate.Struct(&nodeType); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	remote.Normalize(&nodeType)

	if err := a.nodeTypes.Save(c.Context(), &nodeType); err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	return c.JSON(nodeType)
}

func (a *API) getNodeType(c fiber.Ctx) error {
	nodeType, err := a.nodeTypes.Get(c.Context(), c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(nodeType)
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, workflow.ErrWorkflowNotFound),
		errors.Is(err, persistence.ErrExecutionNotFound),
		errors.Is(err, persistence.ErrEventDefinitionNotFound),
		errors.Is(err, persistence.ErrRemoteNodeTypeNotFound):
		return fiber.StatusNotFound
	case errors.Is(err, workflow.ErrInvalidWorkflow):
		return fiber.StatusUnprocessableEntity
	case errors.Is(err, errTenantRequired):
		return fiber.StatusBadRequest
	default:
		return fiber.StatusInternalServerError
	}
}

func problem(c fiber.Ctx, status int, err error) error {
	body := problems.NewStatusProblem(status).WithInstance(c.Path()).WithDetail(err.Error())

	return c.Status(status).JSON(body, "application/problem+json")
}
