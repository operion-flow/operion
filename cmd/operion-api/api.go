// Package main provides the Operion API server implementation.
package main

import (
	"log/slog"
	"strconv"

	"github.com/dukex/operion/pkg/eventbus"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/services"
	"github.com/dukex/operion/pkg/web"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/gofiber/fiber/v3/middleware/healthcheck"
	"github.com/gofiber/fiber/v3/middleware/logger"
)

type API struct {
	logger      *slog.Logger
	persistence persistence.Persistence
	registry    *registry.Registry
	eventBus    eventbus.EventBus
	validate    *validator.Validate
}

func NewAPI(
	logger *slog.Logger,
	persistence persistence.Persistence,
	registry *registry.Registry,
	eventBus eventbus.EventBus,
) *API {
	return &API{
		persistence: persistence,
		logger:      logger,
		registry:    registry,
		eventBus:    eventBus,
		validate:    validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (a *API) App() *fiber.App {
	workflowService := services.NewWorkflow(a.persistence)
	publishingService := services.NewPublishing(a.persistence)
	nodeService := services.NewNode(a.persistence)

	handlers := web.NewAPIHandlers(workflowService, publishingService, nodeService, a.validate, a.registry, a.eventBus)

	app := fiber.New()
	app.Use(cors.New())
	app.Use(logger.New(logger.Config{
		DisableColors: true,
	}))

	app.Get(healthcheck.DefaultLivenessEndpoint, healthcheck.NewHealthChecker())
	app.Get(healthcheck.DefaultReadinessEndpoint, healthcheck.NewHealthChecker())

	app.Get("/", func(c fiber.Ctx) error {
		return c.SendString("Operion API")
	})

	w := app.Group("/workflows")
	w.Get("/", handlers.GetWorkflows)
	w.Post("/", handlers.CreateWorkflow)
	w.Get("/:id", handlers.GetWorkflow)
	w.Patch("/:id", handlers.UpdateWorkflow)
	w.Delete("/:id", handlers.DeleteWorkflow)
	w.Post("/:id/publish", handlers.PublishWorkflow)
	w.Post("/groups/:groupId/create-draft", handlers.CreateDraftFromPublished)

	// Node endpoints:
	w.Post("/:id/nodes", handlers.CreateWorkflowNode)
	w.Get("/:id/nodes/:nodeId", handlers.GetWorkflowNode)
	w.Patch("/:id/nodes/:nodeId", handlers.UpdateWorkflowNode)
	w.Delete("/:id/nodes/:nodeId", handlers.DeleteWorkflowNode)

	app.Get("/health", handlers.HealthCheck)

	return app
}

func (a *API) Start(port int) error {
	app := a.App()

	err := app.Listen(":" + strconv.Itoa(port))

	return err
}
