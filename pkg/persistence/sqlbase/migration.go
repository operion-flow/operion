// Package sqlbase provides the base functionality for SQL database persistence.
package sqlbase

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"slices"
)

// MigrationManager handles database schema migrations.
type MigrationManager struct {
	db         *sql.DB
	logger     *slog.Logger
	migrations map[int]string
}

// NewMigrationManager creates a new migration manager.
func NewMigrationManager(logger *slog.Logger, db *sql.DB, migrations map[int]string) *MigrationManager {
	return &MigrationManager{
		db:         db,
		logger:     logger,
		migrations: migrations,
	}
}

const migrationLockKey = 7206151617

// RunMigrations applies, in version order, every migration not yet recorded in schema_migrations.
// It holds a Postgres advisory lock for the whole run, so replicas starting together apply each migration once.
func (m *MigrationManager) RunMigrations(ctx context.Context) error {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get a connection for migrations: %w", err)
	}

	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("failed to lock migrations: %w", err)
	}

	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrationLockKey)
	}()

	_, err = conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}

	for _, version := range slices.Sorted(maps.Keys(m.migrations)) {
		if applied[version] {
			continue
		}

		if err := m.apply(ctx, conn, version); err != nil {
			return err
		}

		m.logger.InfoContext(ctx, "Migration applied", "version", version)
	}

	return nil
}

func appliedVersions(ctx context.Context, conn *sql.Conn) (map[int]bool, error) {
	rows, err := conn.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to read applied migrations: %w", err)
	}

	defer func() { _ = rows.Close() }()

	applied := make(map[int]bool)

	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("failed to read applied migrations: %w", err)
		}

		applied[version] = true
	}

	return applied, rows.Err()
}

func (m *MigrationManager) apply(ctx context.Context, conn *sql.Conn, version int) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for migration %d: %w", version, err)
	}

	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, m.migrations[version]); err != nil {
		return fmt.Errorf("failed to execute migration %d: %w", version, err)
	}

	if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", version, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration %d: %w", version, err)
	}

	return nil
}
