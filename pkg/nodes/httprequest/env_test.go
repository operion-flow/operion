package httprequest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	urlKey    = "url"
	methodKey = "method"
)

func allowEnv(t *testing.T, values map[string]string) {
	t.Helper()

	names := make([]string, 0, len(values))
	for name, value := range values {
		t.Setenv(name, value)
		names = append(names, name)
	}

	require.NoError(t, template.AllowEnv(names))
	t.Cleanup(func() { require.NoError(t, template.AllowEnv(nil)) })
}

func TestRequestsUseAllowedEnvironmentVariables(t *testing.T) {
	authorization := make(chan string, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization <- r.Header.Get("Authorization")
	}))
	defer server.Close()

	allowEnv(t, map[string]string{"OPERION_TEST_CRM_URL": server.URL, "OPERION_TEST_CRM_TOKEN": "s3cr3t"})

	node, err := NewHTTPRequestNode("crm", map[string]any{
		urlKey:    `{{env "OPERION_TEST_CRM_URL"}}/employees`,
		"headers": map[string]any{"Authorization": `Bearer {{env "OPERION_TEST_CRM_TOKEN"}}`},
	})
	require.NoError(t, err)

	results, err := node.Execute(models.ExecutionContext{}, map[string]models.NodeResult{})
	require.NoError(t, err)

	assert.Contains(t, results, OutputPortSuccess)
	assert.Equal(t, "Bearer s3cr3t", <-authorization)
}

func TestFailedRequestsDoNotRevealTheRenderedURL(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	closed := server.URL
	server.Close()

	allowEnv(t, map[string]string{"OPERION_TEST_HOOK": closed + "/services/s3cr3t"})

	node, err := NewHTTPRequestNode("notify", map[string]any{urlKey: `{{env "OPERION_TEST_HOOK"}}`, methodKey: http.MethodPost})
	require.NoError(t, err)

	results, err := node.Execute(models.ExecutionContext{}, map[string]models.NodeResult{})
	require.NoError(t, err)

	message, ok := results[OutputPortError].Data["error"].(string)
	require.True(t, ok)
	assert.NotContains(t, message, "s3cr3t")
	assert.Contains(t, message, `OPERION_TEST_HOOK`)
}
