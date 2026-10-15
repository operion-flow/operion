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

type API struct {
	workflows  Workflows
	publisher  Publisher
	executions Executions
	validate   *validator.Validate
}

func New(workflows Workflows, publisher Publisher, executions Executions) *API {
	return &API{
		workflows:  workflows,
		publisher:  publisher,
		executions: executions,
		validate:   validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (a *API) Register(app *fiber.App) {
	app.Get("/workflows", a.listWorkflows)
	app.Post("/workflows", a.createWorkflow)
	app.Get("/workflows/:id", a.getWorkflow)
	app.Post("/workflows/:id/publish", a.publishWorkflow)
	app.Get("/executions/:id", a.getExecution)
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

func statusFor(err error) int {
	switch {
	case errors.Is(err, workflow.ErrWorkflowNotFound), errors.Is(err, persistence.ErrExecutionNotFound):
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
