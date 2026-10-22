package secrets_test

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"testing"

	"github.com/dukex/operion/pkg/secrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const crmToken = "crm_token"

func newKey(t *testing.T) string {
	t.Helper()

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	return base64.StdEncoding.EncodeToString(key)
}

func TestCipherRoundTrips(t *testing.T) {
	cipher, err := secrets.NewCipher(newKey(t))
	require.NoError(t, err)

	sealed, err := cipher.Seal("acme", crmToken, "s3cr3t-value")
	require.NoError(t, err)
	assert.False(t, bytes.Contains(sealed, []byte("s3cr3t-value")))

	again, err := cipher.Seal("acme", crmToken, "s3cr3t-value")
	require.NoError(t, err)
	assert.NotEqual(t, sealed, again)

	value, err := cipher.Open("acme", crmToken, sealed)
	require.NoError(t, err)
	assert.Equal(t, "s3cr3t-value", value)
}

func TestCipherBindsTheCiphertextToItsTenantAndName(t *testing.T) {
	cipher, err := secrets.NewCipher(newKey(t))
	require.NoError(t, err)

	sealed, err := cipher.Seal("acme", crmToken, "s3cr3t-value")
	require.NoError(t, err)

	_, err = cipher.Open("globex", crmToken, sealed)
	require.ErrorIs(t, err, secrets.ErrUndecryptable)

	_, err = cipher.Open("acme", "other", sealed)
	require.ErrorIs(t, err, secrets.ErrUndecryptable)

	_, err = cipher.Open("acme", crmToken, sealed[:4])
	require.ErrorIs(t, err, secrets.ErrUndecryptable)

	other, err := secrets.NewCipher(newKey(t))
	require.NoError(t, err)

	_, err = other.Open("acme", crmToken, sealed)
	require.ErrorIs(t, err, secrets.ErrUndecryptable)
}

func TestNewCipherNeedsA32ByteBase64Key(t *testing.T) {
	for _, key := range []string{"", "not base64!", base64.StdEncoding.EncodeToString([]byte("too short"))} {
		_, err := secrets.NewCipher(key)
		require.ErrorIs(t, err, secrets.ErrInvalidKey, key)
	}
}

func TestValidName(t *testing.T) {
	for _, name := range []string{crmToken, "a", "slack2"} {
		assert.True(t, secrets.ValidName(name), name)
	}

	for _, name := range []string{"", "CRM", "2fa", "crm-token", "crm token", string(make([]byte, 65))} {
		assert.False(t, secrets.ValidName(name), name)
	}
}
