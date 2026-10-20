package remote_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	errorPort = "error"
	amount    = "amount"
	employee  = "Ana"
	team      = "Vendas"
	tags      = "tags"
)

func benefitType(url string) models.RemoteNodeType {
	return models.RemoteNodeType{
		ID:      "create_benefit",
		Name:    "Criar benefício",
		URL:     url,
		Outputs: []string{"approved", "rejected"},
		Schema:  map[string]any{"required": []any{amount}},
	}
}

func event() map[string]any {
	return map[string]any{"body": map[string]any{"name": employee}}
}

func execution() models.ExecutionContext {
	return models.ExecutionContext{
		ID:          "exec-1",
		WorkflowID:  "wf-1",
		TriggerData: event(),
		Variables:   map[string]any{"team": team},
		NodeResults: map[string]models.NodeResult{"trigger": {Data: event()}},
	}
}

func input() map[string]models.NodeResult {
	return map[string]models.NodeResult{"main": {Data: map[string]any{"employee": employee}}}
}

func answering(t *testing.T, status int, body string, seen *map[string]any) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen != nil {
			assert.NoError(t, json.NewDecoder(r.Body).Decode(seen))
		}

		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	return server
}

func errorMessage(t *testing.T, outputs map[string]models.NodeResult) string {
	t.Helper()

	require.Contains(t, outputs, errorPort)

	return outputs[errorPort].Error
}

func TestExecuteSendsTheContextAndRoutesToTheAnsweredPort(t *testing.T) {
	var seen map[string]any

	server := answering(t, http.StatusOK, `{"port": "approved", "data": {"benefit_id": "b-1"}}`, &seen)
	config := map[string]any{
		amount:     "{{.variables.team}}",
		tags:       []any{"{{.node_results.trigger.body.name}}", 3},
		"metadata": map[string]any{"source": "operion"},
	}
	node := remote.New(benefitType(server.URL), "benefit", config, server.Client())

	outputs, err := node.Execute(execution(), input())

	require.NoError(t, err)
	require.Contains(t, outputs, "approved")
	assert.Equal(t, map[string]any{"benefit_id": "b-1"}, outputs["approved"].Data)
	assert.Equal(t, string(models.NodeStatusSuccess), outputs["approved"].Status)
	assert.Equal(t, "exec-1", seen["execution_id"])
	assert.Equal(t, "wf-1", seen["workflow_id"])
	assert.Equal(t, "benefit", seen["node_id"])
	assert.Equal(t, map[string]any{amount: team, tags: []any{employee, float64(3)}, "metadata": map[string]any{"source": "operion"}}, seen["config"])
	assert.Equal(t, map[string]any{"employee": employee}, seen["input"])
	assert.Equal(t, event(), seen["trigger_data"])
	assert.Equal(t, map[string]any{"trigger": event()}, seen["node_results"])
	assert.Equal(t, map[string]any{"team": team}, seen["variables"])
}

func TestExecuteSignsTheBodyWhenTheTypeHasASecret(t *testing.T) {
	const secret = "a-secret-of-sixteen-bytes"

	var (
		timestamp, signature string
		body                 []byte
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timestamp = r.Header.Get("X-Operion-Timestamp")
		signature = r.Header.Get("X-Operion-Signature")
		body, _ = io.ReadAll(r.Body)

		_, _ = w.Write([]byte(`{"port": "approved"}`))
	}))
	t.Cleanup(server.Close)

	nodeType := benefitType(server.URL)
	nodeType.Secret = secret

	outputs, err := remote.New(nodeType, "benefit", nil, server.Client()).Execute(execution(), input())
	require.NoError(t, err)
	require.Contains(t, outputs, "approved")

	sent, err := strconv.ParseInt(timestamp, 10, 64)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), time.Unix(sent, 0), 5*time.Second)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "." + string(body)))
	assert.Equal(t, "sha256="+hex.EncodeToString(mac.Sum(nil)), signature)
}

func TestExecuteSendsNoSignatureWithoutASecret(t *testing.T) {
	headers := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()

		_, _ = w.Write([]byte(`{"port": "approved"}`))
	}))
	t.Cleanup(server.Close)

	_, err := remote.New(benefitType(server.URL), "benefit", nil, server.Client()).Execute(execution(), input())
	require.NoError(t, err)

	sent := <-headers
	assert.Empty(t, sent.Get("X-Operion-Signature"))
	assert.Empty(t, sent.Get("X-Operion-Timestamp"))
}

func TestExecuteMarksTheErrorPortAsError(t *testing.T) {
	server := answering(t, http.StatusOK, `{"port": "error", "data": {"reason": "no budget"}}`, nil)

	outputs, err := remote.New(benefitType(server.URL), "benefit", nil, server.Client()).Execute(execution(), input())

	require.NoError(t, err)
	assert.Equal(t, string(models.NodeStatusError), outputs[errorPort].Status)
	assert.Equal(t, map[string]any{"reason": "no budget"}, outputs[errorPort].Data)
}

func TestExecuteFailures(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		message string
	}{
		{name: "unknown port", status: http.StatusOK, body: `{"port": "maybe"}`, message: `remote node answered unknown port "maybe"`},
		{name: "server error", status: http.StatusBadGateway, body: `upstream down`, message: "remote node answered status 502: upstream down"},
		{name: "invalid json", status: http.StatusOK, body: `not json`, message: "remote node answered invalid JSON"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := answering(t, tt.status, tt.body, nil)

			outputs, err := remote.New(benefitType(server.URL), "benefit", nil, server.Client()).Execute(execution(), input())

			require.NoError(t, err)
			assert.Contains(t, errorMessage(t, outputs), tt.message)
		})
	}
}

func TestExecuteTimesOut(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	nodeType := benefitType(server.URL)
	nodeType.TimeoutSeconds = 1

	outputs, err := remote.New(nodeType, "benefit", nil, server.Client()).Execute(execution(), input())

	require.NoError(t, err)
	assert.Contains(t, errorMessage(t, outputs), "context deadline exceeded")
}

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("connection reset") }
func (failingBody) Close() error             { return nil }

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestExecuteReportsTransportAndEncodingErrors(t *testing.T) {
	unreachable := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	})}
	brokenBody := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: failingBody{}}, nil
	})}

	outputs, _ := remote.New(benefitType("http://benefits.test"), "benefit", nil, unreachable).Execute(execution(), input())
	assert.Contains(t, errorMessage(t, outputs), "connection refused")

	outputs, _ = remote.New(benefitType("http://benefits.test"), "benefit", nil, brokenBody).Execute(execution(), input())
	assert.Contains(t, errorMessage(t, outputs), "connection reset")

	outputs, _ = remote.New(benefitType("http://[::1"), "benefit", nil, unreachable).Execute(execution(), input())
	assert.Contains(t, errorMessage(t, outputs), "missing ']'")

	outputs, _ = remote.New(benefitType("http://benefits.test"), "benefit", map[string]any{tags: []any{"{{"}}, unreachable).Execute(execution(), input())
	assert.Contains(t, errorMessage(t, outputs), "config template")

	unencodable := execution()
	unencodable.TriggerData = map[string]any{"channel": make(chan int)}
	outputs, _ = remote.New(benefitType("http://benefits.test"), "benefit", nil, unreachable).Execute(unencodable, input())
	assert.Contains(t, errorMessage(t, outputs), "encode request")
}

func TestNodeDescribesItself(t *testing.T) {
	node := remote.New(benefitType("http://benefits.test"), "benefit", nil, http.DefaultClient)

	assert.Equal(t, "benefit", node.ID())
	assert.Equal(t, "create_benefit", node.Type())
	assert.Equal(t, "benefit:main", node.InputPorts()[0].ID)

	outputs := make([]string, 0, len(node.OutputPorts()))
	for _, port := range node.OutputPorts() {
		outputs = append(outputs, port.Name)
	}

	assert.Equal(t, []string{"approved", "rejected", errorPort}, outputs)
	require.NoError(t, node.Validate(map[string]any{amount: 10}))
	require.EqualError(t, node.Validate(map[string]any{}), `missing required config field "amount"`)
}

func TestNormalizeFillsDefaults(t *testing.T) {
	empty := models.RemoteNodeType{}
	remote.Normalize(&empty)
	assert.Equal(t, []string{"success", errorPort}, empty.Outputs)
	assert.Equal(t, 30, empty.TimeoutSeconds)

	explicit := models.RemoteNodeType{Outputs: []string{"done", errorPort}, TimeoutSeconds: 5}
	remote.Normalize(&explicit)
	assert.Equal(t, []string{"done", errorPort}, explicit.Outputs)
	assert.Equal(t, 5, explicit.TimeoutSeconds)
}
