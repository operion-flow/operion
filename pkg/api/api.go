package api

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/moogar0880/problems"
)

type Workflows interface {
	FetchAll(ctx context.Context) ([]*models.Workflow, error)
	FetchByID(ctx context.Context, id string) (*models.Workflow, error)
	Create(ctx context.Context, workflow *models.Workflow) (*models.Workflow, error)
}

type Publisher interface {
	PublishWorkflow(ctx context.Context, workflowID string) (*models.Workflow, error)
}

type Executions interface {
	GetExecutionContext(ctx context.Context, executionID string) (*models.ExecutionContext, error)
}

type Catalog interface {
	Save(ctx context.Context, definition *models.EventDefinition) error
	Get(ctx context.Context, key string) (*models.EventDefinition, error)
	List(ctx context.Context) ([]*models.EventDefinition, error)
}

type API struct {
	workflows  Workflows
	publisher  Publisher
	executions Executions
	catalog    Catalog
	validate   *validator.Validate
}

func New(workflows Workflows, publisher Publisher, executions Executions, catalog Catalog) *API {
	return &API{
		workflows:  workflows,
		publisher:  publisher,
		executions: executions,
		catalog:    catalog,
		validate:   validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (a *API) Register(app *fiber.App) {
	app.Get("/workflows", a.listWorkflows)
	app.Post("/workflows", a.createWorkflow)
	app.Get("/workflows/:id", a.getWorkflow)
	app.Post("/workflows/:id/publish", a.publishWorkflow)
	app.Get("/executions/:id", a.getExecution)
	app.Put("/platform/event-definitions/:key", a.saveEventDefinition)
	app.Get("/platform/event-definitions/:key", a.getEventDefinition)
	app.Get("/catalog/events", a.listCatalog)
}

func (a *API) listWorkflows(c fiber.Ctx) error {
	workflows, err := a.workflows.FetchAll(c.Context())
	if err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	return c.JSON(workflows)
}

func (a *API) createWorkflow(c fiber.Ctx) error {
	var wf models.Workflow
	if err := json.Unmarshal(c.Body(), &wf); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	wf.Status = models.WorkflowStatusDraft

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
	wf, err := a.workflows.FetchByID(c.Context(), c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(wf)
}

func (a *API) publishWorkflow(c fiber.Ctx) error {
	published, err := a.publisher.PublishWorkflow(c.Context(), c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(published)
}

func (a *API) getExecution(c fiber.Ctx) error {
	execution, err := a.executions.GetExecutionContext(c.Context(), c.Params("id"))
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.JSON(execution)
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

func statusFor(err error) int {
	switch {
	case errors.Is(err, workflow.ErrWorkflowNotFound),
		errors.Is(err, persistence.ErrExecutionNotFound),
		errors.Is(err, persistence.ErrEventDefinitionNotFound):
		return fiber.StatusNotFound
	case errors.Is(err, workflow.ErrInvalidWorkflow):
		return fiber.StatusUnprocessableEntity
	default:
		return fiber.StatusInternalServerError
	}
}

func problem(c fiber.Ctx, status int, err error) error {
	body := problems.NewStatusProblem(status).WithInstance(c.Path()).WithDetail(err.Error())

	return c.Status(status).JSON(body, "application/problem+json")
}
