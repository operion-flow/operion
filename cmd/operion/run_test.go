package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const greetWorkflow = `{
  "id": "greet",
  "name": "greet",
  "nodes": [
    {"id": "trigger", "type": "trigger:webhook", "category": "trigger", "config": {"webhook_path": "/hook"}},
    {"id": "greet", "type": "transform", "category": "action", "config": {"expression": "%s"}}
  ],
  "connections": [{"id": "c1", "source_port": "trigger:success", "target_port": "greet:main"}]
}`

func writeFile(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

func greet(expression string) string {
	return strings.Replace(greetWorkflow, "%s", expression, 1)
}

func run(t *testing.T, args ...string) (map[string]any, error) {
	t.Helper()

	var out bytes.Buffer

	command := newCommand()
	command.Writer = &out

	err := command.Run(t.Context(), append([]string{"operion", "run"}, args...))

	var execution map[string]any
	if out.Len() > 0 {
		require.NoError(t, json.Unmarshal(out.Bytes(), &execution))
	}

	return execution, err
}

func TestRunPrintsCompletedExecution(t *testing.T) {
	workflow := writeFile(t, "workflow.json", greet("hello {{.node_results.trigger.body.name}}"))
	event := writeFile(t, "event.json", `{"body": {"name": "Ana"}}`)

	execution, err := run(t, "--workflow", workflow, "--event", event)

	require.NoError(t, err)
	assert.Equal(t, "completed", execution["status"])
	assert.Equal(t, "hello Ana", execution["node_results"].(map[string]any)["greet"].(map[string]any)["data"].(map[string]any)["result"])
}

func TestRunReadsSecretsFromAFileAndMasksThem(t *testing.T) {
	workflow := writeFile(t, "workflow.json", greet(`Bearer {{secret \"crm_token\"}}`))
	event := writeFile(t, "event.json", `{}`)
	secretsFile := writeFile(t, "secrets.json", `{"crm_token": "s3cr3t-crm-token"}`)

	execution, err := run(t, "--workflow", workflow, "--event", event, "--secrets", secretsFile)
	require.NoError(t, err)
	assert.Equal(t, "Bearer ***", execution["node_results"].(map[string]any)["greet"].(map[string]any)["data"].(map[string]any)["result"])
	assert.NotContains(t, execution, "secrets")

	_, err = run(t, "--workflow", workflow, "--event", event)
	require.ErrorIs(t, err, errExecutionFailed)
	assert.Contains(t, err.Error(), "secret not found: crm_token")
}

func TestRunFailsWhenExecutionFails(t *testing.T) {
	workflow := writeFile(t, "workflow.json", greet("{not json}"))
	event := writeFile(t, "event.json", `{}`)

	execution, err := run(t, "--workflow", workflow, "--event", event)

	require.ErrorIs(t, err, errExecutionFailed)
	assert.Equal(t, "failed", execution["status"])
}

func TestRunRejectsInvalidWorkflow(t *testing.T) {
	workflow := writeFile(t, "workflow.json", strings.Replace(greet("hi"), "trigger:success", "trigger:output", 1))
	event := writeFile(t, "event.json", `{}`)

	_, err := run(t, "--workflow", workflow, "--event", event)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid workflow")
	assert.Contains(t, err.Error(), `output port "trigger:output" does not exist`)
}

func TestRunPicksTrigger(t *testing.T) {
	twoTriggers := `{
  "id": "two",
  "name": "two",
  "nodes": [
    {"id": "first", "type": "trigger:webhook", "category": "trigger", "config": {"webhook_path": "/a"}},
    {"id": "second", "type": "trigger:webhook", "category": "trigger", "config": {"webhook_path": "/b"}}
  ]
}`
	noTrigger := `{"id": "none", "name": "none", "nodes": []}`
	event := writeFile(t, "event.json", `{}`)

	_, err := run(t, "--workflow", writeFile(t, "two.json", twoTriggers), "--event", event)
	require.EqualError(t, err, "workflow has 2 trigger nodes, pass --trigger to pick one")

	_, err = run(t, "--workflow", writeFile(t, "none.json", noTrigger), "--event", event)
	require.EqualError(t, err, "workflow has 0 trigger nodes, pass --trigger to pick one")

	execution, err := run(t, "--workflow", writeFile(t, "two.json", twoTriggers), "--event", event, "--trigger", "second")
	require.NoError(t, err)
	assert.Contains(t, execution["node_results"], "second")
	assert.NotContains(t, execution["node_results"], "first")
}

func TestRunReportsUnreadableFiles(t *testing.T) {
	workflow := writeFile(t, "workflow.json", greet("hi"))
	event := writeFile(t, "event.json", `{}`)
	missing := filepath.Join(t.TempDir(), "missing.json")
	broken := writeFile(t, "broken.json", `{`)

	_, err := run(t, "--workflow", missing, "--event", event)
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = run(t, "--workflow", workflow, "--event", missing)
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = run(t, "--workflow", broken, "--event", event)
	require.ErrorContains(t, err, broken)
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errWrite
}

var errWrite = errors.New("write failed")

func TestRunStopsOnCancelledContextAndWriteErrors(t *testing.T) {
	workflow := writeFile(t, "workflow.json", greet("hi"))
	event := writeFile(t, "event.json", `{}`)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, runWorkflow(ctx, runFiles{workflow: workflow, event: event}, "", &bytes.Buffer{}), context.Canceled)
	require.ErrorIs(t, runWorkflow(t.Context(), runFiles{workflow: workflow, event: event}, "", failingWriter{}), errWrite)
}

func TestRunSkipsWaitsAndFinishesTheExecution(t *testing.T) {
	workflow := writeFile(t, "workflow.json", `{
  "id": "later",
  "name": "later",
  "nodes": [
    {"id": "trigger", "type": "trigger:webhook", "category": "trigger", "config": {"webhook_path": "/hook"}},
    {"id": "hold", "type": "wait", "category": "action", "config": {"duration": "720h"}},
    {"id": "greet", "type": "transform", "category": "action", "config": {"expression": "hello {{.node_results.trigger.body.name}}"}}
  ],
  "connections": [
    {"id": "c1", "source_port": "trigger:success", "target_port": "hold:main"},
    {"id": "c2", "source_port": "hold:success", "target_port": "greet:main"}
  ]
}`)
	event := writeFile(t, "event.json", `{"body": {"name": "Ana"}}`)

	execution, err := run(t, "--workflow", workflow, "--event", event)

	require.NoError(t, err)
	assert.Equal(t, "completed", execution["status"])

	results := execution["node_results"].(map[string]any)
	assert.Equal(t, "success", results["hold"].(map[string]any)["status"])
	assert.Equal(t, "hello Ana", results["greet"].(map[string]any)["data"].(map[string]any)["result"])
}

func TestRunResolvesRemoteNodeTypesFromAFile(t *testing.T) {
	var seen map[string]any

	plugin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&seen))

		_, _ = w.Write([]byte(`{"port": "approved", "data": {"benefit_id": "b-1"}}`))
	}))
	t.Cleanup(plugin.Close)

	nodeTypes := writeFile(t, "node-types.json", `{"create_benefit": {"name": "Criar benefício", "url": "`+plugin.URL+`", "outputs": ["approved"]}}`)
	workflow := writeFile(t, "workflow.json", `{
  "id": "benefit",
  "name": "benefit",
  "nodes": [
    {"id": "trigger", "type": "trigger:webhook", "category": "trigger", "config": {"webhook_path": "/hook"}},
    {"id": "benefit", "type": "create_benefit", "category": "action", "config": {"employee": "{{.trigger_data.body.name}}"}}
  ],
  "connections": [{"id": "c1", "source_port": "trigger:success", "target_port": "benefit:main"}]
}`)
	event := writeFile(t, "event.json", `{"body": {"name": "Ana"}}`)

	execution, err := run(t, "--workflow", workflow, "--event", event, "--node-types", nodeTypes)

	require.NoError(t, err)
	assert.Equal(t, "completed", execution["status"])
	assert.Equal(t, "b-1", execution["node_results"].(map[string]any)["benefit"].(map[string]any)["data"].(map[string]any)["benefit_id"])
	assert.Equal(t, map[string]any{"employee": "Ana"}, seen["config"])

	_, err = run(t, "--workflow", workflow, "--event", event)
	require.ErrorContains(t, err, "node type not registered")

	_, err = run(t, "--workflow", workflow, "--event", event, "--node-types", writeFile(t, "bad.json", `{"create_benefit": {"url": "`+plugin.URL+`"}}`))
	require.ErrorContains(t, err, `node type "create_benefit"`)
}
