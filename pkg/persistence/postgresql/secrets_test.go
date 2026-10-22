package postgresql_test

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"testing"

	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/secrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const crmTokenValue = "s3cr3t-crm-token"

func newCipher(t *testing.T) *secrets.Cipher {
	t.Helper()

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	cipher, err := secrets.NewCipher(base64.StdEncoding.EncodeToString(key))
	require.NoError(t, err)

	return cipher
}

func TestSecretsAreStoredEncryptedPerTenant(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	store := p.Secrets(newCipher(t))

	require.NoError(t, store.Save(ctx, acmeTenant, "crm_token", "first-value"))
	require.NoError(t, store.Save(ctx, acmeTenant, "crm_token", crmTokenValue))
	require.NoError(t, store.Save(ctx, "globex", "slack_url", "https://hooks.example/globex"))

	values, err := store.Values(ctx, acmeTenant)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"crm_token": crmTokenValue}, values)

	list, err := store.List(ctx, acmeTenant)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "crm_token", list[0].Name)
	assert.False(t, list[0].UpdatedAt.IsZero())

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	var stored []byte
	require.NoError(t, db.QueryRowContext(ctx, `SELECT ciphertext FROM secrets WHERE tenant_id = $1 AND name = $2`, acmeTenant, "crm_token").Scan(&stored))
	assert.False(t, bytes.Contains(stored, []byte(crmTokenValue)))

	require.ErrorIs(t, store.Delete(ctx, "globex", "crm_token"), persistence.ErrSecretNotFound)
	require.NoError(t, store.Delete(ctx, acmeTenant, "crm_token"))
	require.ErrorIs(t, store.Delete(ctx, acmeTenant, "crm_token"), persistence.ErrSecretNotFound)

	values, err = store.Values(ctx, acmeTenant)
	require.NoError(t, err)
	assert.Empty(t, values)
}

func TestSecretsMovedToAnotherTenantCannotBeRead(t *testing.T) {
	p, ctx, databaseURL := setupTestDB(t)
	store := p.Secrets(newCipher(t))

	require.NoError(t, store.Save(ctx, acmeTenant, "crm_token", crmTokenValue))

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(ctx, `UPDATE secrets SET tenant_id = 'globex' WHERE tenant_id = $1`, acmeTenant)
	require.NoError(t, err)

	_, err = store.Values(ctx, "globex")
	require.ErrorIs(t, err, secrets.ErrUndecryptable)
}

func TestSecretsNeedTheKeyTheyWereSavedWith(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	require.NoError(t, p.Secrets(newCipher(t)).Save(ctx, acmeTenant, "crm_token", crmTokenValue))

	_, err := p.Secrets(newCipher(t)).Values(ctx, acmeTenant)
	require.ErrorIs(t, err, secrets.ErrUndecryptable)
}
