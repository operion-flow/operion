package ingest_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	callbackToken  = "the-token"
	callbackTarget = "/callbacks/exec-1/benefit/" + callbackToken
	approvedAnswer = `{"port": "approved"}`
)

type resumed struct {
	executionID string
	nodeID      string
	data        map[string]any
}

type fakeResumer struct {
	waiting models.NodeResult
	resumed []resumed
	err     error
}

func (f *fakeResumer) ResumeIf(_ context.Context, executionID, nodeID string, data map[string]any, allowed func(models.NodeResult) bool) error {
	if f.err != nil {
		return f.err
	}

	if !allowed(f.waiting) {
		return persistence.ErrResumeRefused
	}

	f.resumed = append(f.resumed, resumed{executionID: executionID, nodeID: nodeID, data: data})

	return nil
}

func waitingFor(token string) models.NodeResult {
	sum := sha256.Sum256([]byte(token))

	return models.NodeResult{Status: string(models.NodeStatusWaiting), Data: map[string]any{"callback_token_sha256": hex.EncodeToString(sum[:])}}
}

func callback(t *testing.T, resumer *fakeResumer, target, body string) int {
	t.Helper()

	app := fiber.New()
	ingest.NewCallbacks(resumer).Register(app)

	resp, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(body)))
	require.NoError(t, err)

	_ = resp.Body.Close()

	return resp.StatusCode
}

func TestCallbackResumesTheNodeWithTheAnswer(t *testing.T) {
	resumer := &fakeResumer{waiting: waitingFor(callbackToken)}

	status := callback(t, resumer, callbackTarget, `{"port": "approved", "data": {"benefit_id": "b-1"}}`)

	require.Equal(t, http.StatusAccepted, status)
	assert.Equal(t, []resumed{{
		executionID: "exec-1",
		nodeID:      "benefit",
		data:        map[string]any{"port": "approved", "data": map[string]any{"benefit_id": "b-1"}},
	}}, resumer.resumed)
}

func TestCallbackRefusals(t *testing.T) {
	tests := []struct {
		name    string
		waiting models.NodeResult
		err     error
		target  string
		body    string
		status  int
	}{
		{name: "wrong token", waiting: waitingFor(callbackToken), target: "/callbacks/exec-1/benefit/other", body: approvedAnswer, status: http.StatusNotFound},
		{name: "node holds no token", waiting: models.NodeResult{Data: map[string]any{"resume_at": "soon"}}, target: callbackTarget, body: approvedAnswer, status: http.StatusNotFound},
		{name: "unknown execution", err: fmt.Errorf("%w: exec-1", persistence.ErrExecutionNotFound), target: callbackTarget, body: approvedAnswer, status: http.StatusNotFound},
		{name: "already answered", err: persistence.ErrNodeNotWaiting, target: callbackTarget, body: approvedAnswer, status: http.StatusConflict},
		{name: "store down", err: errors.New("store is down"), target: callbackTarget, body: approvedAnswer, status: http.StatusInternalServerError},
		{name: "no port", waiting: waitingFor(callbackToken), target: callbackTarget, body: `{"data": {}}`, status: http.StatusBadRequest},
		{name: "not json", waiting: waitingFor(callbackToken), target: callbackTarget, body: `approved`, status: http.StatusBadRequest},
		{name: "data is not an object", waiting: waitingFor(callbackToken), target: callbackTarget, body: `{"port": "approved", "data": [1]}`, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resumer := &fakeResumer{waiting: tt.waiting, err: tt.err}

			assert.Equal(t, tt.status, callback(t, resumer, tt.target, tt.body))
			assert.Empty(t, resumer.resumed)
		})
	}
}
