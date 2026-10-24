package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/remote"
	"github.com/dukex/operion/pkg/protocol"
	"github.com/dukex/operion/pkg/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	secret      = "a-secret-of-sixteen-bytes"
	benefitNode = "benefit"
)

func startPlugin(t *testing.T) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(plugin{secret: secret, client: http.DefaultClient, logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(server.Close)

	return server
}

func benefits(t *testing.T, url, signWith, callbackBase, amount string) *remote.Node {
	t.Helper()

	nodeType := models.RemoteNodeType{ID: "grant_benefit", Name: "Grant benefit", URL: url, Secret: signWith, Outputs: []string{portApproved}}

	return remote.New(nodeType, benefitNode, map[string]any{amountKey: amount}, http.DefaultClient, callbackBase)
}

func execution() models.ExecutionContext {
	return models.ExecutionContext{ID: "exec-1", WorkflowID: "wf-1"}
}

func TestSmallAmountsAreApprovedRightAway(t *testing.T) {
	server := startPlugin(t)

	outputs, err := benefits(t, server.URL, secret, "", "100").Execute(execution(), nil)

	require.NoError(t, err)
	require.Contains(t, outputs, portApproved)
	assert.Equal(t, map[string]any{"benefit_id": "exec-1/benefit", amountKey: float64(100)}, outputs[portApproved].Data)
}

func TestCallsSignedWithAnotherSecretAreRefused(t *testing.T) {
	server := startPlugin(t)

	for _, signWith := range []string{"", "another-secret-of-sixteen"} {
		outputs, err := benefits(t, server.URL, signWith, "", "100").Execute(execution(), nil)

		require.NoError(t, err)
		require.Contains(t, outputs, portError, signWith)
		assert.Contains(t, outputs[portError].Error, "status 401", signWith)
	}
}

func TestInvalidAmountsGoToTheErrorPort(t *testing.T) {
	server := startPlugin(t)

	outputs, err := benefits(t, server.URL, secret, "", "lots").Execute(execution(), nil)

	require.NoError(t, err)
	assert.Equal(t, "amount must be a positive number", outputs[portError].Data["error"])
}

func TestLargeAmountsAreAnsweredThroughTheCallback(t *testing.T) {
	type callback struct {
		path string
		body []byte
	}

	received := make(chan callback, 1)
	operion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- callback{path: r.URL.Path, body: body}

		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(operion.Close)

	node := benefits(t, startPlugin(t).URL, secret, operion.URL, "1000")

	outputs, err := node.Execute(execution(), nil)

	assert.Nil(t, outputs)

	var held *protocol.SuspendError
	require.ErrorAs(t, err, &held)
	assert.WithinDuration(t, time.Now().Add(approvalHours*time.Hour), held.Until, 5*time.Second)

	var got callback

	select {
	case got = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("the plugin did not call back")
	}

	assert.True(t, strings.HasPrefix(got.path, "/callbacks/exec-1/benefit/"), got.path)

	var answer map[string]any
	require.NoError(t, json.Unmarshal(got.body, &answer))

	outputs, err = node.Execute(execution(), map[string]models.NodeResult{protocol.ResumePort: {Data: answer}})

	require.NoError(t, err)
	require.Contains(t, outputs, portApproved)
	assert.Equal(t, map[string]any{"benefit_id": "exec-1/benefit", amountKey: float64(1000)}, outputs[portApproved].Data)
}

func TestLargeAmountsWithoutACallbackURLGoToTheErrorPort(t *testing.T) {
	outputs, err := benefits(t, startPlugin(t).URL, secret, "", "1000").Execute(execution(), nil)

	require.NoError(t, err)
	assert.Equal(t, "amounts above the limit need a callback_url", outputs[portError].Data["error"])
}

func TestVerifiedRefusesOldTimestampsOtherBodiesAndOtherSecrets(t *testing.T) {
	body := []byte(`{"execution_id": "exec-1"}`)
	now := time.Unix(1_800_000_000, 0)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("1800000000." + string(body)))

	header := http.Header{}
	header.Set("X-Operion-Timestamp", "1800000000")
	header.Set("X-Operion-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))

	assert.True(t, verified(secret, header, body, now))
	assert.False(t, verified(secret, header, []byte(`{}`), now))
	assert.False(t, verified(secret, header, body, now.Add(6*time.Minute)))
	assert.False(t, verified("another-secret-of-sixteen", header, body, now))
	assert.False(t, verified(secret, http.Header{}, body, now))
}

func readJSON(t *testing.T, path string, target any) {
	t.Helper()

	content, err := os.ReadFile(filepath.Clean(path))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(content, target))
}

func TestTheExampleWorkflowGrantsTheBenefit(t *testing.T) {
	types, err := remote.ReadTypes("node-types.json")
	require.NoError(t, err)

	var wf models.Workflow

	readJSON(t, "workflow.json", &wf)

	grant := types["grant_benefit"]
	require.NotEmpty(t, grant.Secret)

	server := httptest.NewServer(plugin{secret: grant.Secret, client: http.DefaultClient, logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(server.Close)

	grant.URL = server.URL
	types["grant_benefit"] = grant

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()
	reg.UseRemoteTypes(types)

	e := engine.New(reg)
	require.NoError(t, e.Validate(t.Context(), &wf))

	event := map[string]any{"body": map[string]any{amountKey: 100}}
	execution := &models.ExecutionContext{ID: "exec-1", WorkflowID: wf.ID, Status: models.ExecutionStatusRunning, TriggerData: event}

	deferred, err := e.Run(t.Context(), &wf, execution, engine.TriggerActivation("trigger", event))

	require.NoError(t, err)
	assert.Empty(t, deferred)
	require.Equal(t, models.ExecutionStatusCompleted, execution.Status, execution.ErrorMessage)
	assert.Equal(t, "benefit exec-1/benefit granted", execution.NodeResults["done"].Data["message"])
}
