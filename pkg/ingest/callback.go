package ingest

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/remote"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/gofiber/fiber/v3"
)

type Resumer interface {
	ResumeIf(ctx context.Context, executionID, nodeID string, data map[string]any, allowed func(models.NodeResult) bool) error
}

type Callbacks struct {
	jobs Resumer
}

func NewCallbacks(jobs Resumer) *Callbacks {
	return &Callbacks{jobs: jobs}
}

func (h *Callbacks) Register(app *fiber.App) {
	app.Post("/callbacks/:execution_id/:node_id/:token", h.handle)
}

func (h *Callbacks) handle(c fiber.Ctx) error {
	var answer struct {
		Port string         `json:"port"`
		Data map[string]any `json:"data"`
	}

	if err := json.Unmarshal(c.Body(), &answer); err != nil || answer.Port == "" {
		return fiber.NewError(fiber.StatusBadRequest, `the body must be {"port": "<output>", "data": {...}}`)
	}

	token := c.Params("token")
	err := h.jobs.ResumeIf(c.Context(), c.Params("execution_id"), c.Params("node_id"),
		map[string]any{"port": answer.Port, "data": answer.Data},
		func(result models.NodeResult) bool { return remote.CallbackAllowed(result, token) })

	switch {
	case errors.Is(err, persistence.ErrExecutionNotFound), errors.Is(err, persistence.ErrResumeRefused):
		return fiber.NewError(fiber.StatusNotFound, "no call waits on this callback URL")
	case errors.Is(err, persistence.ErrNodeNotWaiting):
		return fiber.NewError(fiber.StatusConflict, "the call was already answered or its deadline passed")
	case err != nil:
		return err
	}

	return c.SendStatus(fiber.StatusAccepted)
}
