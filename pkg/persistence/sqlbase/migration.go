// Package sqlbase provides the base functionality for SQL database persistence.
package sqlbase

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
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

// getTargetSchemaVersion returns the highest version number from the migrations map.
func (m *MigrationManager) getTargetSchemaVersion() int {
	maxVersion := 0
	for version := range m.migrations {
		if version > maxVersion {
			maxVersion = version
		}
	}

	return maxVersion
}

// RunMigrations handles database schema creation and updates.
func (m *MigrationManager) RunMigrations(ctx context.Context) error {
	m.logger.InfoContext(ctx, "Starting database migrations")

	err := m.createMigrationsTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	currentVersion, err := m.getCurrentSchemaVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current schema version: %w", err)
	}

	targetVersion := m.getTargetSchemaVersion()

	m.logger.InfoContext(ctx, "Current schema version", "version", currentVersion)

	if currentVersion < targetVersion {
		err := m.applyMigrations(ctx, currentVersion)
		if err != nil {
			return fmt.Errorf("failed to apply migrations: %w", err)
		}
	}

	m.logger.InfoContext(ctx, "Database migrations completed", "version", targetVersion)

	return nil
}

func (m *MigrationManager) createMigrationsTable(ctx context.Context) error {
	createMigrationsSQL := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		);
	`

	_, err := m.db.ExecContext(ctx, createMigrationsSQL)
	if err != nil {
		m.logger.ErrorContext(ctx, "Failed to create schema_migrations table", "error", err)

		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	m.logger.InfoContext(ctx, "Schema migrations table created successfully")

	return nil
}

// getCurrentSchemaVersion returns the current schema version.
func (m *MigrationManager) getCurrentSchemaVersion(ctx context.Context) (int, error) {
	var version int

	err := m.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to query current schema version: %w", err)
	}

	return version, nil
}

// applyMigrations applies all migrations from the current version to the latest in sorted order.
func (m *MigrationManager) applyMigrations(ctx context.Context, fromVersion int) error {
	// Get sorted list of versions to ensure migrations run in correct order
	var versions []int

	for version := range m.migrations {
		if version > fromVersion {
			versions = append(versions, version)
		}
	}

	// Sort versions in ascending order to ensure proper migration sequence
	sort.Ints(versions)

	// Apply migrations in sorted order
	for _, version := range versions {
		migration := m.migrations[version]
		m.logger.InfoContext(ctx, "Applying migration", "version", version)

		transaction, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", version, err)
		}

		_, err = transaction.ExecContext(ctx, migration)
		if err != nil {
			_ = transaction.Rollback()

			return fmt.Errorf("failed to execute migration %d: %w", version, err)
		}

		// Record migration
		_, err = transaction.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version)
		if err != nil {
			_ = transaction.Rollback()

			return fmt.Errorf("failed to record migration %d: %w", version, err)
		}

		err = transaction.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", version, err)
		}

		m.logger.InfoContext(ctx, "Migration applied successfully", "version", version)
	}

	return nil
}
