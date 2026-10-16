package postgresql_test

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/dukex/operion/pkg/persistence/sqlbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationsApplyVersionsMergedOutOfOrder(t *testing.T) {
	_, ctx, databaseURL := setupTestDB(t)

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS migration_test_one, migration_test_two, migration_test_three")
		_ = db.Close()
	})

	logger := slog.New(slog.DiscardHandler)
	one := "CREATE TABLE migration_test_one (id INT)"
	two := "CREATE TABLE migration_test_two (id INT)"
	three := "CREATE TABLE migration_test_three (id INT)"

	require.NoError(t, sqlbase.NewMigrationManager(logger, db, map[int]string{1001: one, 1003: three}).RunMigrations(ctx))
	require.NoError(t, sqlbase.NewMigrationManager(logger, db, map[int]string{1001: one, 1002: two, 1003: three}).RunMigrations(ctx))

	var exists bool

	require.NoError(t, db.QueryRowContext(ctx, `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'migration_test_two')`).Scan(&exists))
	assert.True(t, exists)

	var recorded int

	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version > 1000`).Scan(&recorded))
	assert.Equal(t, 3, recorded)
}
