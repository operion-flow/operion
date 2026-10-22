package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
)

const keySize = 32

var (
	ErrInvalidKey    = errors.New("the secrets key must be the base64 encoding of 32 bytes")
	ErrUndecryptable = errors.New("secret cannot be decrypted with this key")

	namePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

// Cipher encrypts secret values with AES-256-GCM, binding each ciphertext to its tenant and name.
type Cipher struct {
	aead cipher.AEAD
}

// NewCipher builds a Cipher from the base64 encoding of a 32-byte key.
func NewCipher(encodedKey string) (*Cipher, error) {
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(key) != keySize {
		return nil, ErrInvalidKey
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create the secrets cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create the secrets cipher: %w", err)
	}

	return &Cipher{aead: aead}, nil
}

// Seal encrypts value with a random nonce, which it prepends to the ciphertext.
func (c *Cipher) Seal(tenantID, name, value string) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate a nonce: %w", err)
	}

	return c.aead.Seal(nonce, nonce, []byte(value), additionalData(tenantID, name)), nil
}

// Open decrypts what Seal produced for the same tenant and name.
func (c *Cipher) Open(tenantID, name string, sealed []byte) (string, error) {
	size := c.aead.NonceSize()
	if len(sealed) < size {
		return "", fmt.Errorf("%w: %s", ErrUndecryptable, name)
	}

	plain, err := c.aead.Open(nil, sealed[:size], sealed[size:], additionalData(tenantID, name))
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrUndecryptable, name)
	}

	return string(plain), nil
}

// ValidName reports whether name can name a secret: a lowercase letter, then up to 63 lowercase letters, digits or underscores.
func ValidName(name string) bool {
	return namePattern.MatchString(name)
}

func additionalData(tenantID, name string) []byte {
	return []byte(tenantID + "\x00" + name)
}
