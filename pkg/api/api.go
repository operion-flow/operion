package api

import (
	"context"
	"crypto/subtle"
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

var (
	errTenantRequired  = errors.New(tenantHeader + " header is required")
	errAnswerNeedsPort = errors.New(`this node waits for its remote service to call back; resume it with the answer, {"port": "<output>", "data": {...}}`)
	errUnauthorized    = errors.New("a valid bearer token is required")
)

type Workflows interface {
	FetchAll(ctx context.Context, tenantID string) ([]*models.Workflow, error)
	FetchByID(ctx context.Context, id string) (*models.Workflow, error)
	Create(ctx context.Context, workflow *models.Workflow) (*models.Workflow, error)
}

type Publisher interface {
	PublishWorkflow(ctx context.Context, workflowID string) (*models.Workflow, error)
	UnpublishWorkflow(ctx context.Context, workflowID string) (*models.Workflow, error)
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

type Resumer interface {
	Resume(ctx context.Context, executionID, nodeID string, data map[string]any) error
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
	resumer    Resumer
	nodeTypes  NodeTypes
	validate   *validator.Validate
}

func New(workflows Workflows, publisher Publisher, executions Executions, catalog Catalog, resumer Resumer, nodeTypes NodeTypes) *API {
	return &API{
		workflows:  workflows,
		publisher:  publisher,
		executions: executions,
		catalog:    catalog,
		resumer:    resumer,
		nodeTypes:  nodeTypes,
		validate:   validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (a *API) Register(app *fiber.App, token string) {
	private := authorized(token)

	app.Get("/workflows", private(a.listWorkflows))
	app.Post("/workflows", private(a.createWorkflow))
	app.Get("/workflows/:id", private(a.getWorkflow))
	app.Post("/workflows/:id/publish", private(a.publishWorkflow))
	app.Post("/workflows/:id/unpublish", private(a.unpublishWorkflow))
	app.Get("/workflows/:id/executions", private(a.listExecutions))
	app.Get("/executions/:id", private(a.getExecution))
	app.Post("/executions/:id/nodes/:node_id/resume", private(a.resumeNode))
	app.Put("/platform/event-definitions/:key", private(a.saveEventDefinition))
	app.Get("/platform/event-definitions/:key", private(a.getEventDefinition))
	app.Get("/catalog/events", private(a.listCatalog))
	app.Put("/platform/node-types/:id", private(a.saveNodeType))
	app.Get("/platform/node-types/:id", private(a.getNodeType))
}

func authorized(token string) func(fiber.Handler) fiber.Handler {
	expected := []byte("Bearer " + token)

	return func(next fiber.Handler) fiber.Handler {
		return func(c fiber.Ctx) error {
			if token == "" || subtle.ConstantTimeCompare([]byte(c.Get(fiber.HeaderAuthorization)), expected) != 1 {
				c.Set(fiber.HeaderWWWAuthenticate, "Bearer")

				return problem(c, fiber.StatusUnauthorized, errUnauthorized)
			}

			return next(c)
		}
	}
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

	if wf.WorkflowGroupID != "" {
		if _, err := a.ownedWorkflow(c, wf.WorkflowGroupID); err != nil {
			return problem(c, statusFor(err), err)
		}
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

func (a *API) unpublishWorkflow(c fiber.Ctx) error {
	wf, err := a.ownedWorkflow(c, c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	unpublished, err := a.publisher.UnpublishWorkflow(c.Context(), wf.ID)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(unpublished)
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
	execution, err := a.ownedExecution(c)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(execution)
}

func (a *API) ownedExecution(c fiber.Ctx) (*models.ExecutionContext, error) {
	execution, err := a.executions.GetExecutionContext(c.Context(), c.Params("id"))
	if err != nil {
		return nil, err
	}

	if _, err := a.ownedWorkflow(c, execution.WorkflowID); err != nil {
		if errors.Is(err, workflow.ErrWorkflowNotFound) {
			err = fmt.Errorf("%w: %s", persistence.ErrExecutionNotFound, execution.ID)
		}

		return nil, err
	}

	return execution, nil
}

func (a *API) resumeNode(c fiber.Ctx) error {
	var data map[string]any

	if len(c.Body()) > 0 {
		if err := json.Unmarshal(c.Body(), &data); err != nil {
			return problem(c, fiber.StatusBadRequest, err)
		}
	}

	execution, err := a.ownedExecution(c)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	if port, _ := data["port"].(string); port == "" && remote.AwaitsCallback(execution.NodeResults[c.Params("node_id")]) {
		return problem(c, fiber.StatusBadRequest, errAnswerNeedsPort)
	}

	if err := a.resumer.Resume(c.Context(), execution.ID, c.Params("node_id"), data); err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"execution": execution.ID})
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

	if err := remote.Prepare(&nodeType); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	if err := a.nodeTypes.Save(c.Context(), &nodeType); err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	return c.JSON(withoutSecret(nodeType))
}

func (a *API) getNodeType(c fiber.Ctx) error {
	nodeType, err := a.nodeTypes.Get(c.Context(), c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(withoutSecret(*nodeType))
}

func withoutSecret(nodeType models.RemoteNodeType) models.RemoteNodeType {
	nodeType.Secret = ""

	return nodeType
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, workflow.ErrWorkflowNotFound),
		errors.Is(err, persistence.ErrExecutionNotFound),
		errors.Is(err, persistence.ErrEventDefinitionNotFound),
		errors.Is(err, persistence.ErrRemoteNodeTypeNotFound),
		errors.Is(err, persistence.ErrSecretNotFound):
		return fiber.StatusNotFound
	case errors.Is(err, workflow.ErrInvalidWorkflow):
		return fiber.StatusUnprocessableEntity
	case errors.Is(err, persistence.ErrNodeNotWaiting),
		errors.Is(err, persistence.ErrWorkflowNotPublished):
		return fiber.StatusConflict
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
