package remote_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dukex/operion/pkg/nodes/remote"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func typesFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "node-types.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

func TestReadTypesPreparesEveryEntry(t *testing.T) {
	types, err := remote.ReadTypes(typesFile(t, `{
		"create_benefit": {"name": "Create benefit", "url": "http://benefits.test", "outputs": ["granted"], "schema": {"required": ["amount"]}},
		"notify_manager": {"name": "Notify manager", "url": "http://managers.test", "timeout_seconds": 5}
	}`))
	require.NoError(t, err)

	benefit, err := types.Get(t.Context(), "create_benefit")
	require.NoError(t, err)
	assert.Equal(t, "create_benefit", benefit.ID)
	assert.Equal(t, []string{"granted", errorPort}, benefit.Outputs)
	assert.Equal(t, 30, benefit.TimeoutSeconds)
	assert.Equal(t, []any{amount}, benefit.Schema["required"])

	manager, err := types.Get(t.Context(), "notify_manager")
	require.NoError(t, err)
	assert.Equal(t, []string{"success", errorPort}, manager.Outputs)
	assert.Equal(t, 5, manager.TimeoutSeconds)
}

func TestReadTypesRejectsInvalidFiles(t *testing.T) {
	_, err := remote.ReadTypes(filepath.Join(t.TempDir(), "missing.json"))
	require.ErrorIs(t, err, os.ErrNotExist)

	broken := typesFile(t, `{`)
	_, err = remote.ReadTypes(broken)
	require.ErrorContains(t, err, broken)

	_, err = remote.ReadTypes(typesFile(t, `{"create_benefit": {"name": "Create benefit", "url": "not a url"}}`))
	require.ErrorContains(t, err, `node type "create_benefit"`)
	require.ErrorContains(t, err, "URL")

	_, err = remote.ReadTypes(typesFile(t, `{"slow": {"name": "Lento", "url": "http://slow.test", "timeout_seconds": 301}}`))
	require.ErrorContains(t, err, `node type "slow"`)
}

func TestTypesGetUnknownType(t *testing.T) {
	_, err := remote.Types{}.Get(t.Context(), "create_benefit")

	require.ErrorIs(t, err, persistence.ErrRemoteNodeTypeNotFound)
}
