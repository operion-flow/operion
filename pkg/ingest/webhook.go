package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

type Triggers interface {
	WebhookTriggers(ctx context.Context, tenantID, path string) ([]models.TriggerNodeMatch, error)
}

type Starter interface {
	Start(ctx context.Context, execution *models.ExecutionContext, job models.Job) error
}

type Webhook struct {
	triggers Triggers
	jobs     Starter
	logger   *slog.Logger
}

func NewWebhook(triggers Triggers, jobs Starter, logger *slog.Logger) *Webhook {
	return &Webhook{triggers: triggers, jobs: jobs, logger: logger}
}

func (h *Webhook) Register(app *fiber.App) {
	app.Post("/webhooks/:tenant/*", h.handle)
}

func (h *Webhook) handle(c fiber.Ctx) error {
	ctx := c.Context()
	tenant := c.Params("tenant")
	path := "/" + c.Params("*")

	matches, err := h.triggers.WebhookTriggers(ctx, tenant, path)
	if err != nil {
		return err
	}

	if len(matches) == 0 {
		return fiber.NewError(fiber.StatusNotFound, "no published workflow listens on "+path)
	}

	data, err := requestData(c)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	executions := make([]string, 0, len(matches))

	for _, match := range matches {
		now := time.Now().UTC()
		activation := engine.TriggerActivation(match.TriggerNode.ID, data)
		execution := &models.ExecutionContext{
			ID:          uuid.NewString(),
			WorkflowID:  match.WorkflowID,
			Status:      models.ExecutionStatusRunning,
			NodeResults: map[string]models.NodeResult{},
			TriggerData: data,
			CreatedAt:   now,
		}
		job := models.Job{
			ID:          uuid.NewString(),
			ExecutionID: execution.ID,
			NodeID:      activation.NodeID,
			Port:        activation.Port,
			Data:        activation.Data,
			RunAt:       now,
		}

		if err := h.jobs.Start(ctx, execution, job); err != nil {
			return err
		}

		h.logger.InfoContext(ctx, "execution started",
			"execution_id", execution.ID,
			"workflow_id", execution.WorkflowID,
			"node_id", activation.NodeID,
			"webhook_path", path,
		)

		executions = append(executions, execution.ID)
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"executions": executions})
}

func requestData(c fiber.Ctx) (map[string]any, error) {
	var body any

	if raw := c.Body(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, fmt.Errorf("body is not valid JSON: %w", err)
		}
	}

	headers := make(map[string]any)
	for name, values := range c.GetReqHeaders() {
		headers[name] = strings.Join(values, ", ")
	}

	query := make(map[string]any)
	for name, value := range c.Queries() {
		query[name] = value
	}

	return map[string]any{
		"body":    body,
		"headers": headers,
		"query":   query,
		"method":  c.Method(),
		"url":     c.OriginalURL(),
	}, nil
}
