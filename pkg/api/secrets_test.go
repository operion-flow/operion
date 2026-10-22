package api_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/api"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	crmTokenValue = "s3cr3t-crm-token"
	crmRoute      = "/secrets/crm_token"
	smallValue    = `{"value": "x"}`
)

type fakeSecretStore struct {
	values map[string]map[string]string
	err    error
}

func (f *fakeSecretStore) Save(_ context.Context, tenantID, name, value string) error {
	if f.err != nil {
		return f.err
	}

	if f.values[tenantID] == nil {
		f.values[tenantID] = map[string]string{}
	}

	f.values[tenantID][name] = value

	return nil
}

func (f *fakeSecretStore) List(_ context.Context, tenantID string) ([]models.Secret, error) {
	list := make([]models.Secret, 0, len(f.values[tenantID]))
	for name := range f.values[tenantID] {
		list = append(list, models.Secret{Name: name, UpdatedAt: time.Unix(0, 0).UTC()})
	}

	return list, f.err
}

func (f *fakeSecretStore) Delete(_ context.Context, tenantID, name string) error {
	if _, ok := f.values[tenantID][name]; !ok {
		return fmt.Errorf("%w: %s", persistence.ErrSecretNotFound, name)
	}

	delete(f.values[tenantID], name)

	return nil
}

func secretsRequest(t *testing.T, store *fakeSecretStore, tenant, method, target, body string) (int, string) {
	t.Helper()

	app := fiber.New()
	api.NewSecrets(store).Register(app, apiToken)

	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+apiToken)

	if tenant != "" {
		req.Header.Set("X-Tenant-Id", tenant)
	}

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	content, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(content)
}

func TestSecretsAreWriteOnly(t *testing.T) {
	store := &fakeSecretStore{values: map[string]map[string]string{}}

	status, _ := secretsRequest(t, store, acmeTenant, http.MethodPut, crmRoute, `{"value": "`+crmTokenValue+`"}`)
	require.Equal(t, http.StatusNoContent, status)
	assert.Equal(t, crmTokenValue, store.values[acmeTenant]["crm_token"])

	status, body := secretsRequest(t, store, acmeTenant, http.MethodGet, "/secrets", "")
	require.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `[{"name": "crm_token", "updated_at": "1970-01-01T00:00:00Z"}]`, body)
	assert.NotContains(t, body, crmTokenValue)

	status, body = secretsRequest(t, store, acmeTenant, http.MethodGet, crmRoute, "")
	assert.Equal(t, http.StatusMethodNotAllowed, status)
	assert.NotContains(t, body, crmTokenValue)
}

func TestSecretsBelongToTheCallerTenant(t *testing.T) {
	store := &fakeSecretStore{values: map[string]map[string]string{acmeTenant: {"crm_token": crmTokenValue}}}

	status, body := secretsRequest(t, store, "globex", http.MethodGet, "/secrets", "")
	require.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `[]`, body)

	status, _ = secretsRequest(t, store, "globex", http.MethodDelete, crmRoute, "")
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, crmTokenValue, store.values[acmeTenant]["crm_token"])

	status, _ = secretsRequest(t, store, acmeTenant, http.MethodDelete, crmRoute, "")
	assert.Equal(t, http.StatusNoContent, status)
	assert.Empty(t, store.values[acmeTenant])
}

func TestSaveSecretRejectsBadRequests(t *testing.T) {
	tests := []struct {
		name   string
		tenant string
		target string
		body   string
		err    error
		status int
	}{
		{name: "no tenant", target: crmRoute, body: smallValue, status: http.StatusBadRequest},
		{name: "invalid name", tenant: acmeTenant, target: "/secrets/CRM-token", body: smallValue, status: http.StatusBadRequest},
		{name: "empty value", tenant: acmeTenant, target: crmRoute, body: `{"value": ""}`, status: http.StatusBadRequest},
		{name: "not json", tenant: acmeTenant, target: crmRoute, body: `{`, status: http.StatusBadRequest},
		{name: "store fails", tenant: acmeTenant, target: crmRoute, body: smallValue, err: errStore, status: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeSecretStore{values: map[string]map[string]string{}, err: tt.err}

			status, _ := secretsRequest(t, store, tt.tenant, http.MethodPut, tt.target, tt.body)

			assert.Equal(t, tt.status, status)
			assert.Empty(t, store.values)
		})
	}
}

func TestSecretRoutesRequireTheBearerToken(t *testing.T) {
	app := fiber.New()
	api.NewSecrets(&fakeSecretStore{values: map[string]map[string]string{}}).Register(app, apiToken)

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		target := crmRoute
		if method == http.MethodGet {
			target = "/secrets"
		}

		req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(smallValue))
		req.Header.Set("X-Tenant-Id", acmeTenant)

		resp, err := app.Test(req)
		require.NoError(t, err)

		_ = resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, method)
	}
}
