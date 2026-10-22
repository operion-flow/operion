package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/secrets"
)

type Secrets struct {
	db     *sql.DB
	cipher *secrets.Cipher
}

func (p *Persistence) Secrets(cipher *secrets.Cipher) *Secrets {
	return &Secrets{db: p.db, cipher: cipher}
}

func (s *Secrets) Save(ctx context.Context, tenantID, name, value string) error {
	sealed, err := s.cipher.Seal(tenantID, name, value)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO secrets (tenant_id, name, ciphertext, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (tenant_id, name) DO UPDATE SET
			ciphertext = EXCLUDED.ciphertext,
			updated_at = EXCLUDED.updated_at
	`, tenantID, name, sealed, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to save secret %s: %w", name, err)
	}

	return nil
}

func (s *Secrets) Delete(ctx context.Context, tenantID, name string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM secrets WHERE tenant_id = $1 AND name = $2`, tenantID, name)
	if err != nil {
		return fmt.Errorf("failed to delete secret %s: %w", name, err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to delete secret %s: %w", name, err)
	}

	if deleted == 0 {
		return fmt.Errorf("%w: %s", persistence.ErrSecretNotFound, name)
	}

	return nil
}

func (s *Secrets) List(ctx context.Context, tenantID string) ([]models.Secret, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, updated_at FROM secrets WHERE tenant_id = $1 ORDER BY name`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list secrets: %w", err)
	}

	defer func() { _ = rows.Close() }()

	list := []models.Secret{}

	for rows.Next() {
		var secret models.Secret
		if err := rows.Scan(&secret.Name, &secret.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to read secret: %w", err)
		}

		list = append(list, secret)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to list secrets: %w", err)
	}

	return list, nil
}

func (s *Secrets) Values(ctx context.Context, tenantID string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, ciphertext FROM secrets WHERE tenant_id = $1`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to load secrets: %w", err)
	}

	defer func() { _ = rows.Close() }()

	values := map[string]string{}

	for rows.Next() {
		var (
			name   string
			sealed []byte
		)

		if err := rows.Scan(&name, &sealed); err != nil {
			return nil, fmt.Errorf("failed to read secret: %w", err)
		}

		value, err := s.cipher.Open(tenantID, name, sealed)
		if err != nil {
			return nil, err
		}

		values[name] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to load secrets: %w", err)
	}

	return values, nil
}
