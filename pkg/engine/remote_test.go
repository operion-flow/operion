package engine_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	employeeField = "employee"
	employeeName  = "Ana"
)

type remoteTypes map[string]*models.RemoteNodeType

func (r remoteTypes) Get(_ context.Context, id string) (*models.RemoteNodeType, error) {
	nodeType, ok := r[id]
	if !ok {
		return nil, persistence.ErrRemoteNodeTypeNotFound
	}

	return nodeType, nil
}

func remoteEngine(url string) *engine.Engine {
	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()
	reg.UseRemoteTypes(remoteTypes{"create_benefit": {
		ID:      "create_benefit",
		URL:     url,
		Outputs: []string{"approved", "rejected"},
		Schema:  map[string]any{"required": []any{employeeField}},
	}})

	return engine.New(reg)
}

func benefitWorkflow(config map[string]any) *models.Workflow {
	return workflow(
		[]*models.WorkflowNode{trigger(), node("benefit", "create_benefit", config), logNode("approved"), logNode("rejected")},
		connect("trigger:success", "benefit:main"),
		connect("benefit:approved", "approved:main"),
		connect("benefit:rejected", "rejected:main"),
	)
}

func TestRemoteNodeRunsInsideTheWorkflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Config map[string]any `json:"config"`
		}

		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		_, _ = w.Write([]byte(`{"port": "approved", "data": {"employee": "` + body.Config[employeeField].(string) + `"}}`))
	}))
	t.Cleanup(server.Close)

	e := remoteEngine(server.URL)
	wf := benefitWorkflow(map[string]any{employeeField: "{{.trigger_data.body.name}}"})

	require.NoError(t, e.Validate(t.Context(), wf))

	execution := run(t, e, wf, map[string]any{"name": employeeName})

	assert.Equal(t, map[string]any{employeeField: employeeName}, execution.NodeResults["benefit"].Data)
	assert.Contains(t, execution.NodeResults, "approved")
	assert.NotContains(t, execution.NodeResults, "rejected")
}

func TestValidateChecksRemoteNodeConfig(t *testing.T) {
	err := remoteEngine("http://benefits.test").Validate(t.Context(), benefitWorkflow(map[string]any{}))

	require.ErrorContains(t, err, `missing required config field "employee"`)
}
