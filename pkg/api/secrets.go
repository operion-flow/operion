package api

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/secrets"
	"github.com/gofiber/fiber/v3"
)

var (
	errInvalidSecretName = errors.New("a secret name is a lowercase letter followed by up to 63 lowercase letters, digits or underscores")
	errEmptySecretValue  = errors.New("value is required")
)

type SecretStore interface {
	Save(ctx context.Context, tenantID, name, value string) error
	List(ctx context.Context, tenantID string) ([]models.Secret, error)
	Delete(ctx context.Context, tenantID, name string) error
}

type Secrets struct {
	store SecretStore
}

func NewSecrets(store SecretStore) *Secrets {
	return &Secrets{store: store}
}

func (s *Secrets) Register(app *fiber.App, token string) {
	private := authorized(token)

	app.Get("/secrets", private(s.list))
	app.Put("/secrets/:name", private(s.save))
	app.Delete("/secrets/:name", private(s.delete))
}

func (s *Secrets) list(c fiber.Ctx) error {
	tenant, err := tenantOf(c)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	list, err := s.store.List(c.Context(), tenant)
	if err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	return c.JSON(list)
}

func (s *Secrets) save(c fiber.Ctx) error {
	tenant, err := tenantOf(c)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	name := c.Params("name")
	if !secrets.ValidName(name) {
		return problem(c, fiber.StatusBadRequest, errInvalidSecretName)
	}

	var body struct {
		Value string `json:"value"`
	}

	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return problem(c, fiber.StatusBadRequest, err)
	}

	if body.Value == "" {
		return problem(c, fiber.StatusBadRequest, errEmptySecretValue)
	}

	if err := s.store.Save(c.Context(), tenant, name, body.Value); err != nil {
		return problem(c, fiber.StatusInternalServerError, err)
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func (s *Secrets) delete(c fiber.Ctx) error {
	tenant, err := tenantOf(c)
	if err != nil {
		return problem(c, statusFor(err), err)
	}

	if err := s.store.Delete(c.Context(), tenant, c.Params("name")); err != nil {
		return problem(c, statusFor(err), err)
	}

	return c.SendStatus(fiber.StatusNoContent)
}
