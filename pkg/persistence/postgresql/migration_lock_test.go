package postgresql_test

import (
	"database/sql"
	"log/slog"
	"sync"
	"testing"

	"github.com/dukex/operion/pkg/persistence/sqlbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationsRunOnceWhenReplicasStartTogether(t *testing.T) {
	_, ctx, databaseURL := setupTestDB(t)

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS migration_lock_test")
		_ = db.Close()
	})

	migrations := map[int]string{2001: "CREATE TABLE migration_lock_test (id INT); SELECT pg_sleep(0.2)"}
	logger := slog.New(slog.DiscardHandler)

	var (
		wait sync.WaitGroup
		errs = make([]error, 3)
	)

	for i := range errs {
		wait.Add(1)

		go func() {
			defer wait.Done()

			errs[i] = sqlbase.NewMigrationManager(logger, db, migrations).RunMigrations(ctx)
		}()
	}

	wait.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	var recorded int

	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = 2001`).Scan(&recorded))
	assert.Equal(t, 1, recorded)
}
