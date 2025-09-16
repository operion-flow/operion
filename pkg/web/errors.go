package web

import (
	"errors"

	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/services"
	"github.com/gofiber/fiber/v3"
	"github.com/moogar0880/problems"
)

func badRequest(c fiber.Ctx, detail string) error {
	problem := problems.NewStatusProblem(400).
		WithInstance(c.Path()).
		WithType("validation_error").
		WithDetail(detail)

	return c.Status(fiber.StatusBadRequest).JSON(problem)
}

func notFound(c fiber.Ctx, detail string) error {
	problem := problems.NewStatusProblem(404).
		WithInstance(c.Path()).
		WithType("not_found").
		WithDetail(detail)

	return c.Status(fiber.StatusNotFound).JSON(problem)
}

func internalError(c fiber.Ctx, err error) error {
	problem := problems.NewStatusProblem(500).
		WithInstance(c.Path()).
		WithType("internal_error").
		WithError(err)

	return c.Status(fiber.StatusInternalServerError).JSON(problem)
}

// handleServiceError provides typed error handling for service layer errors.
func handleServiceError(c fiber.Ctx, err error) error {
	switch {
	case services.IsValidationError(err):
		// Check for specific node errors to provide better error messages
		if errors.Is(err, services.ErrNodeNotFound) {
			problem := problems.NewStatusProblem(404).
				WithInstance(c.Path()).
				WithType("node_not_found").
				WithDetail("node not found")

			return c.Status(fiber.StatusNotFound).JSON(problem)
		}

		// Default validation error handling
		problem := problems.NewStatusProblem(400).
			WithInstance(c.Path()).
			WithType("validation_error").
			WithDetail(err.Error())

		return c.Status(fiber.StatusBadRequest).JSON(problem)

	case services.IsConflictError(err):
		problem := problems.NewStatusProblem(409).
			WithInstance(c.Path()).
			WithType("conflict").
			WithDetail(err.Error())

		return c.Status(fiber.StatusConflict).JSON(problem)

	case persistence.IsWorkflowNotFound(err):
		problem := problems.NewStatusProblem(404).
			WithInstance(c.Path()).
			WithType("workflow_not_found").
			WithDetail("workflow not found")

		return c.Status(fiber.StatusNotFound).JSON(problem)

	case persistence.IsNodeNotFound(err):
		problem := problems.NewStatusProblem(404).
			WithInstance(c.Path()).
			WithType("node_not_found").
			WithDetail("node not found")

		return c.Status(fiber.StatusNotFound).JSON(problem)

	case persistence.IsPublishedWorkflowNotFound(err):
		problem := problems.NewStatusProblem(404).
			WithInstance(c.Path()).
			WithType("published_workflow_not_found").
			WithDetail("published workflow not found")

		return c.Status(fiber.StatusNotFound).JSON(problem)

	case persistence.IsDraftWorkflowNotFound(err):
		problem := problems.NewStatusProblem(404).
			WithInstance(c.Path()).
			WithType("draft_workflow_not_found").
			WithDetail("draft workflow not found")

		return c.Status(fiber.StatusNotFound).JSON(problem)

	default:
		// Log unexpected errors but don't expose details
		problem := problems.NewStatusProblem(500).
			WithInstance(c.Path()).
			WithType("internal_error").
			WithError(err)

		return c.Status(fiber.StatusInternalServerError).JSON(problem)
	}
}
