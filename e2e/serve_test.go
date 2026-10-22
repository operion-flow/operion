package e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	statusKey   = "status"
	jsonType    = "application/json"
	webhookPath = "/employees"
	tenant      = "acme"
	apiToken    = "e2e-token"
	approved    = "approved"
)

var (
	setupOnce sync.Once
	errSetup  error
	workDir   string
	binary    string
	adminURL  string
	container *postgres.PostgresContainer
)

func TestMain(m *testing.M) {
	code := m.Run()

	if container != nil {
		_ = container.Terminate(context.Background())
	}

	if workDir != "" {
		_ = os.RemoveAll(workDir)
	}

	os.Exit(code)
}

func setup(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("starts Postgres and the operion binary")
	}

	if os.Getenv("TEST_DATABASE_URL") == "" {
		testcontainers.SkipIfProviderIsNotHealthy(t)
	}

	setupOnce.Do(func() { errSetup = prepare() })
	require.NoError(t, errSetup)
}

func prepare() error {
	ctx := context.Background()

	var err error

	workDir, err = os.MkdirTemp("", "operion-e2e")
	if err != nil {
		return err
	}

	binary = filepath.Join(workDir, "operion")

	output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, "../cmd/operion").CombinedOutput() //nolint:gosec // builds into a temp dir this test owns
	if err != nil {
		return fmt.Errorf("build operion: %w: %s", err, output)
	}

	adminURL = os.Getenv("TEST_DATABASE_URL")
	if adminURL != "" {
		return nil
	}

	container, err = postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("operion"),
		postgres.WithUsername("operion"),
		postgres.WithPassword("operion"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return err
	}

	adminURL, err = container.ConnectionString(ctx, "sslmode=disable")

	return err
}

func freshDatabase(t *testing.T) string {
	t.Helper()

	name := "e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	db, err := sql.Open("postgres", adminURL)
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE DATABASE "+name)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = db.Close()
	})

	parsed, err := url.Parse(adminURL)
	require.NoError(t, err)

	parsed.Path = "/" + name

	return parsed.String()
}

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())

	return port
}

func startServe(t *testing.T, flags ...string) string {
	t.Helper()

	setup(t)

	return serveOn(t, freshDatabase(t), flags...)
}

func serveOn(t *testing.T, databaseURL string, flags ...string) string {
	t.Helper()

	port := strconv.Itoa(freePort(t))

	var output bytes.Buffer

	args := append([]string{"serve", "--port", port, "--poll-interval", "100ms", "--log-level", "warn"}, flags...)
	cmd := exec.CommandContext(context.WithoutCancel(t.Context()), binary, args...) //nolint:gosec // runs the binary this test just built

	cmd.Env = append(os.Environ(), "DATABASE_URL="+databaseURL, "OPERION_API_TOKEN="+apiToken, "OPERION_PUBLIC_URL=http://127.0.0.1:"+port)
	cmd.Stdout = &output
	cmd.Stderr = &output

	require.NoError(t, cmd.Start())

	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)

		err := cmd.Wait()

		if t.Failed() {
			t.Logf("operion serve output:\n%s", output.String())
		}

		assert.NoError(t, err, "operion serve did not stop cleanly")
	})

	base := "http://127.0.0.1:" + port

	require.Eventually(t, func() bool {
		status, _, err := fetch(t.Context(), http.MethodGet, base+"/readyz", nil)

		return err == nil && status == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond)

	return base
}

func fetch(ctx context.Context, method, target string, body any) (int, map[string]any, error) {
	var reader io.Reader

	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}

		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, nil, err
	}

	req.Header.Set("Content-Type", jsonType)
	req.Header.Set("X-Tenant-Id", tenant)
	req.Header.Set("Authorization", "Bearer "+apiToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}

	var decoded map[string]any

	_ = json.Unmarshal(content, &decoded)

	return resp.StatusCode, decoded, nil
}

func call(t *testing.T, method, target string, body any) (int, map[string]any) {
	t.Helper()

	status, decoded, err := fetch(t.Context(), method, target, body)
	require.NoError(t, err)

	return status, decoded
}

func node(id, nodeType, category string, config map[string]any) map[string]any {
	return map[string]any{"id": id, "name": id, "type": nodeType, "category": category, "enabled": true, "config": config}
}

func connection(source, target string) map[string]any {
	return map[string]any{"id": source + "->" + target, "source_port": source, "target_port": target}
}

func workflow(nodes []map[string]any, connections ...map[string]any) map[string]any {
	return map[string]any{"name": "Employee Criado", "description": "greets new employees", "nodes": nodes, "connections": connections}
}

func webhookTrigger() map[string]any {
	return node("trigger", "trigger:webhook", "trigger", map[string]any{"webhook_path": webhookPath})
}

func TestWebhookRunsAWorkflowThatCallsAnExternalAPI(t *testing.T) {
	type request struct {
		method      string
		path        string
		contentType string
		body        string
	}

	received := make(chan request, 1)

	external := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- request{method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type"), body: string(body)}

		w.Header().Set("Content-Type", jsonType)
		_, _ = w.Write([]byte(`{"greeting": "Olá, Ana"}`))
	}))
	defer external.Close()

	base := startServe(t)

	definition := workflow(
		[]map[string]any{
			webhookTrigger(),
			node("notify", "httprequest", "action", map[string]any{
				"url":    external.URL + "/welcome",
				"method": http.MethodPost,
				"body":   `{"employee": "{{.node_results.trigger.body.employee}}"}`,
			}),
			node("welcome", "transform", "action", map[string]any{"expression": "{{.node_results.notify.json.greeting}}"}),
			node("done", "log", "action", map[string]any{"message": "{{.node_results.welcome.result}}"}),
		},
		connection("trigger:success", "notify:main"),
		connection("notify:success", "welcome:main"),
		connection("welcome:success", "done:main"),
	)

	status, created := call(t, http.MethodPost, base+"/workflows", definition)
	require.Equal(t, http.StatusCreated, status, created)

	status, published := call(t, http.MethodPost, base+"/workflows/"+created["id"].(string)+"/publish", nil)
	require.Equal(t, http.StatusOK, status, published)
	assert.Equal(t, "published", published[statusKey])

	status, accepted := call(t, http.MethodPost, base+"/webhooks/"+tenant+webhookPath, map[string]any{"employee": "Ana"})
	require.Equal(t, http.StatusAccepted, status, accepted)

	executionID := accepted["executions"].([]any)[0].(string)

	var execution map[string]any

	require.Eventually(t, func() bool {
		code, decoded, err := fetch(t.Context(), http.MethodGet, base+"/executions/"+executionID, nil)
		if err != nil || code != http.StatusOK {
			return false
		}

		execution = decoded

		return execution[statusKey] != "running"
	}, 30*time.Second, 100*time.Millisecond)

	require.Equal(t, "completed", execution[statusKey], execution["error_message"])

	results := execution["node_results"].(map[string]any)
	data := func(nodeID string) map[string]any {
		return results[nodeID].(map[string]any)["data"].(map[string]any)
	}

	assert.InDelta(t, http.StatusOK, data("notify")["status_code"], 0)
	assert.Equal(t, "Olá, Ana", data("welcome")["result"])
	assert.Equal(t, "Olá, Ana", data("done")["message"])

	select {
	case got := <-received:
		assert.Equal(t, http.MethodPost, got.method)
		assert.Equal(t, "/welcome", got.path)
		assert.Equal(t, jsonType, got.contentType)
		assert.JSONEq(t, `{"employee": "Ana"}`, got.body)
	default:
		t.Fatal("the external API was not called")
	}
}

func TestPublishRejectsAnUndeclaredPort(t *testing.T) {
	base := startServe(t)

	definition := workflow(
		[]map[string]any{webhookTrigger(), node("done", "log", "action", map[string]any{"message": "hi"})},
		connection("trigger:output", "done:main"),
	)

	status, created := call(t, http.MethodPost, base+"/workflows", definition)
	require.Equal(t, http.StatusCreated, status, created)

	id := created["id"].(string)

	status, problem := call(t, http.MethodPost, base+"/workflows/"+id+"/publish", nil)
	require.Equal(t, http.StatusUnprocessableEntity, status, problem)
	assert.Contains(t, problem["detail"], `output port "trigger:output" does not exist`)

	status, stored := call(t, http.MethodGet, base+"/workflows/"+id, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "draft", stored[statusKey])
}

func finished(t *testing.T, base, executionID string) map[string]any {
	t.Helper()

	var execution map[string]any

	require.Eventually(t, func() bool {
		code, decoded, err := fetch(t.Context(), http.MethodGet, base+"/executions/"+executionID, nil)
		if err != nil || code != http.StatusOK {
			return false
		}

		execution = decoded

		return execution[statusKey] != "running"
	}, 30*time.Second, 100*time.Millisecond)

	return execution
}

func startAsyncBenefit(t *testing.T, base string, plugin http.HandlerFunc) string {
	t.Helper()

	server := httptest.NewServer(plugin)
	t.Cleanup(server.Close)

	status, saved := call(t, http.MethodPut, base+"/platform/node-types/create_benefit", map[string]any{
		"name": "Criar benefício", "url": server.URL, "outputs": []string{approved, "rejected"},
	})
	require.Equal(t, http.StatusOK, status, saved)

	definition := workflow(
		[]map[string]any{
			webhookTrigger(),
			node("benefit", "create_benefit", "action", map[string]any{"employee": "{{.node_results.trigger.body.employee}}"}),
			node("done", "log", "action", map[string]any{"message": "granted {{.node_results.benefit.benefit_id}}"}),
		},
		connection("trigger:success", "benefit:main"),
		connection("benefit:approved", "done:main"),
	)

	status, created := call(t, http.MethodPost, base+"/workflows", definition)
	require.Equal(t, http.StatusCreated, status, created)

	status, published := call(t, http.MethodPost, base+"/workflows/"+created["id"].(string)+"/publish", nil)
	require.Equal(t, http.StatusOK, status, published)

	status, accepted := call(t, http.MethodPost, base+"/webhooks/"+tenant+webhookPath, map[string]any{"employee": "Ana"})
	require.Equal(t, http.StatusAccepted, status, accepted)

	return accepted["executions"].([]any)[0].(string)
}

func TestARemoteNodeAnswersLaterThroughItsCallback(t *testing.T) {
	base := startServe(t)
	answered := make(chan int, 2)

	executionID := startAsyncBenefit(t, base, func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			CallbackURL string         `json:"callback_url"`
			Config      map[string]any `json:"config"`
		}

		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&request)) {
			return
		}

		w.WriteHeader(http.StatusAccepted)

		ctx := context.WithoutCancel(r.Context())

		go func() {
			answer := map[string]any{"port": approved, "data": map[string]any{"benefit_id": "b-1", "employee": request.Config["employee"]}}

			status, _, err := fetch(ctx, http.MethodPost, request.CallbackURL, answer)
			assert.NoError(t, err)

			answered <- status

			status, _, err = fetch(ctx, http.MethodPost, request.CallbackURL, answer)
			assert.NoError(t, err)

			answered <- status
		}()
	})

	execution := finished(t, base, executionID)
	require.Equal(t, "completed", execution[statusKey], execution["error_message"])

	results := execution["node_results"].(map[string]any)
	assert.Equal(t, map[string]any{"benefit_id": "b-1", "employee": "Ana"}, results["benefit"].(map[string]any)["data"])
	assert.Equal(t, "granted b-1", results["done"].(map[string]any)["data"].(map[string]any)["message"])

	assert.Equal(t, http.StatusAccepted, <-answered)
	assert.Contains(t, []int{http.StatusNotFound, http.StatusConflict}, <-answered)
}

func TestARemoteNodeThatNeverAnswersFailsAtItsDeadline(t *testing.T) {
	base := startServe(t)

	executionID := startAsyncBenefit(t, base, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"timeout_seconds": 1}`))
	})

	execution := finished(t, base, executionID)
	assert.Equal(t, "failed", execution[statusKey])
	assert.Contains(t, execution["error_message"], "did not call back before its deadline")
}

func TestServeRegistersNodeTypesFromAFile(t *testing.T) {
	nodeTypes := filepath.Join(t.TempDir(), "node-types.json")
	require.NoError(t, os.WriteFile(nodeTypes, []byte(`{"create_benefit": {"name": "Criar benefício", "url": "http://benefits.internal/operion", "outputs": ["approved"]}}`), 0o600))

	base := startServe(t, "--node-types", nodeTypes)

	status, nodeType := call(t, http.MethodGet, base+"/platform/node-types/create_benefit", nil)
	require.Equal(t, http.StatusOK, status, nodeType)
	assert.Equal(t, "Criar benefício", nodeType["name"])
	assert.Equal(t, []any{approved, "error"}, nodeType["outputs"])
}

func TestOnlyTheAPINeedsTheToken(t *testing.T) {
	base := startServe(t)

	statusWithout := func(method, path string) int {
		req, err := http.NewRequestWithContext(t.Context(), method, base+path, strings.NewReader(`{}`))
		require.NoError(t, err)

		req.Header.Set("Content-Type", jsonType)
		req.Header.Set("X-Tenant-Id", tenant)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)

		_ = resp.Body.Close()

		return resp.StatusCode
	}

	assert.Equal(t, http.StatusUnauthorized, statusWithout(http.MethodGet, "/workflows"))
	assert.Equal(t, http.StatusUnauthorized, statusWithout(http.MethodPut, "/platform/node-types/create_benefit"))
	assert.Equal(t, http.StatusOK, statusWithout(http.MethodGet, "/readyz"))
	assert.Equal(t, http.StatusNotFound, statusWithout(http.MethodPost, "/webhooks/"+tenant+webhookPath))

	status, _ := call(t, http.MethodGet, base+"/workflows", nil)
	assert.Equal(t, http.StatusOK, status)
}

func TestReadinessFollowsTheDatabase(t *testing.T) {
	setup(t)

	databaseURL := freshDatabase(t)
	base := serveOn(t, databaseURL)

	parsed, err := url.Parse(databaseURL)
	require.NoError(t, err)

	db, err := sql.Open("postgres", adminURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(t.Context(), "DROP DATABASE "+strings.TrimPrefix(parsed.Path, "/")+" WITH (FORCE)")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		status, _, err := fetch(t.Context(), http.MethodGet, base+"/readyz", nil)

		return err == nil && status == http.StatusServiceUnavailable
	}, 10*time.Second, 100*time.Millisecond)

	status, _ := call(t, http.MethodGet, base+"/livez", nil)
	assert.Equal(t, http.StatusOK, status)
}
