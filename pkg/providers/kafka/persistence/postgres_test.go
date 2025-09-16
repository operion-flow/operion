//go:build integration
// +build integration

package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/dukex/operion/pkg/providers/kafka/models"
)

var postgresContainer *postgres.PostgresContainer

func TestMain(m *testing.M) {
	code := m.Run()

	// Cleanup
	if postgresContainer != nil {
		_ = postgresContainer.Terminate(context.Background())
	}

	os.Exit(code)
}

// setupTestDB creates a test PostgreSQL database for testing.
func setupTestDB(t *testing.T) (*PostgresPersistence, context.Context, string) {
	ctx := context.Background()

	// Use existing container if available and running
	if postgresContainer == nil || !postgresContainer.IsRunning() {
		var err error
		postgresContainer, err = postgres.Run(ctx,
			"postgres:16-alpine",
			postgres.WithDatabase("operion_kafka_test"),
			postgres.WithUsername("operion"),
			postgres.WithPassword("operion"),
			postgres.BasicWaitStrategies(),
		)
		require.NoError(t, err)
	}

	databaseURL, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	persistence, err := NewPostgresPersistence(ctx, logger, databaseURL)
	require.NoError(t, err)

	// Clean up the table before each test
	cleanupDB(t, databaseURL)

	return persistence, ctx, databaseURL
}

func cleanupDB(t *testing.T, databaseURL string) {
	ctx := context.Background()

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, "TRUNCATE TABLE kafka_sources")
	require.NoError(t, err)
}

func TestNewPostgresPersistence(t *testing.T) {
	tests := []struct {
		name        string
		databaseURL string
		expectError bool
	}{
		{
			name:        "valid connection",
			databaseURL: "", // Will be set by setupTestDB
			expectError: false,
		},
		{
			name:        "invalid connection string",
			databaseURL: "postgres://invalid:invalid@nonexistent:5432/nonexistent",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

			if tt.databaseURL == "" {
				// Use test database
				_, _, databaseURL := setupTestDB(t)
				tt.databaseURL = databaseURL
				defer cleanupDB(t, databaseURL)
			}

			persistence, err := NewPostgresPersistence(ctx, logger, tt.databaseURL)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, persistence)
			} else {
				require.NoError(t, err)
				require.NotNil(t, persistence)

				// Test health check
				err = persistence.HealthCheck()
				assert.NoError(t, err)

				// Cleanup
				err = persistence.Close()
				assert.NoError(t, err)
			}
		})
	}
}

func TestPostgresPersistence_SaveAndRetrieveKafkaSource(t *testing.T) {
	persistence, _, databaseURL := setupTestDB(t)
	defer persistence.Close()
	defer cleanupDB(t, databaseURL)

	// Create test source
	config := map[string]any{
		"topic":   "orders",
		"brokers": []string{"localhost:9092"},
		"json_schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"order_id": map[string]any{"type": "string"},
			},
		},
	}
	source, err := models.NewKafkaSource("test-source", config)
	require.NoError(t, err)

	// Save source
	err = persistence.SaveKafkaSource(source)
	require.NoError(t, err)

	// Retrieve by ID
	retrievedSource, err := persistence.KafkaSourceByID("test-source")
	require.NoError(t, err)
	require.NotNil(t, retrievedSource)

	// Verify source data
	assert.Equal(t, source.ID, retrievedSource.ID)
	assert.Equal(t, source.ConnectionDetailsID, retrievedSource.ConnectionDetailsID)
	assert.Equal(t, source.ConnectionDetails, retrievedSource.ConnectionDetails)
	assert.Equal(t, source.JSONSchema, retrievedSource.JSONSchema)
	// Use JSON comparison to handle type differences from serialization
	assertJSONEqual(t, source.Configuration, retrievedSource.Configuration)
	assert.Equal(t, source.Active, retrievedSource.Active)
	assert.True(t, !retrievedSource.CreatedAt.IsZero())
	assert.True(t, !retrievedSource.UpdatedAt.IsZero())

	// Test non-existent source
	nonExistent, err := persistence.KafkaSourceByID("non-existent")
	require.NoError(t, err)
	assert.Nil(t, nonExistent)
}

func TestPostgresPersistence_KafkaSourceByConnectionDetailsID(t *testing.T) {
	persistence, _, databaseURL := setupTestDB(t)
	defer persistence.Close()
	defer cleanupDB(t, databaseURL)

	// Create test sources with same connection details
	config1 := map[string]any{
		"topic":   "orders",
		"brokers": []string{"localhost:9092"},
	}
	source1, err := models.NewKafkaSource("source-1", config1)
	require.NoError(t, err)

	source2, err := models.NewKafkaSource("source-2", config1)
	require.NoError(t, err)

	// Create source with different connection details
	config2 := map[string]any{
		"topic":   "events",
		"brokers": []string{"localhost:9092"},
	}
	source3, err := models.NewKafkaSource("source-3", config2)
	require.NoError(t, err)

	// Save all sources
	err = persistence.SaveKafkaSource(source1)
	require.NoError(t, err)
	err = persistence.SaveKafkaSource(source2)
	require.NoError(t, err)
	err = persistence.SaveKafkaSource(source3)
	require.NoError(t, err)

	// Retrieve sources by connection details ID
	sources1, err := persistence.KafkaSourceByConnectionDetailsID(source1.ConnectionDetailsID)
	require.NoError(t, err)
	assert.Len(t, sources1, 2)

	// Verify correct sources returned
	sourceIDs := []string{sources1[0].ID, sources1[1].ID}
	assert.Contains(t, sourceIDs, "source-1")
	assert.Contains(t, sourceIDs, "source-2")

	// Retrieve source with different connection details
	sources2, err := persistence.KafkaSourceByConnectionDetailsID(source3.ConnectionDetailsID)
	require.NoError(t, err)
	assert.Len(t, sources2, 1)
	assert.Equal(t, "source-3", sources2[0].ID)

	// Test non-existent connection details ID
	nonExistent, err := persistence.KafkaSourceByConnectionDetailsID("non-existent")
	require.NoError(t, err)
	assert.Empty(t, nonExistent)
}

func TestPostgresPersistence_KafkaSources(t *testing.T) {
	persistence, _, databaseURL := setupTestDB(t)
	defer persistence.Close()
	defer cleanupDB(t, databaseURL)

	// Initially should be empty
	sources, err := persistence.KafkaSources()
	require.NoError(t, err)
	assert.Empty(t, sources)

	// Create and save test sources
	config1 := map[string]any{"topic": "orders", "brokers": []string{"localhost:9092"}}
	source1, err := models.NewKafkaSource("source-1", config1)
	require.NoError(t, err)

	config2 := map[string]any{"topic": "events", "brokers": []string{"localhost:9092"}}
	source2, err := models.NewKafkaSource("source-2", config2)
	require.NoError(t, err)

	err = persistence.SaveKafkaSource(source1)
	require.NoError(t, err)
	err = persistence.SaveKafkaSource(source2)
	require.NoError(t, err)

	// Retrieve all sources
	sources, err = persistence.KafkaSources()
	require.NoError(t, err)
	assert.Len(t, sources, 2)

	// Verify correct sources returned
	sourceIDs := []string{sources[0].ID, sources[1].ID}
	assert.Contains(t, sourceIDs, "source-1")
	assert.Contains(t, sourceIDs, "source-2")
}

func TestPostgresPersistence_ActiveKafkaSources(t *testing.T) {
	persistence, _, databaseURL := setupTestDB(t)
	defer persistence.Close()
	defer cleanupDB(t, databaseURL)

	// Create and save active source
	config1 := map[string]any{"topic": "orders", "brokers": []string{"localhost:9092"}}
	activeSource, err := models.NewKafkaSource("active-source", config1)
	require.NoError(t, err)
	err = persistence.SaveKafkaSource(activeSource)
	require.NoError(t, err)

	// Create and save inactive source
	config2 := map[string]any{"topic": "events", "brokers": []string{"localhost:9092"}}
	inactiveSource, err := models.NewKafkaSource("inactive-source", config2)
	require.NoError(t, err)
	inactiveSource.Active = false
	err = persistence.SaveKafkaSource(inactiveSource)
	require.NoError(t, err)

	// Retrieve only active sources
	activeSources, err := persistence.ActiveKafkaSources()
	require.NoError(t, err)
	assert.Len(t, activeSources, 1)
	assert.Equal(t, "active-source", activeSources[0].ID)
	assert.True(t, activeSources[0].Active)

	// Verify all sources still exist
	allSources, err := persistence.KafkaSources()
	require.NoError(t, err)
	assert.Len(t, allSources, 2)
}

func TestPostgresPersistence_UpdateKafkaSource(t *testing.T) {
	persistence, _, databaseURL := setupTestDB(t)
	defer persistence.Close()
	defer cleanupDB(t, databaseURL)

	// Create and save initial source
	config := map[string]any{"topic": "orders", "brokers": []string{"localhost:9092"}}
	source, err := models.NewKafkaSource("test-source", config)
	require.NoError(t, err)
	err = persistence.SaveKafkaSource(source)
	require.NoError(t, err)

	// Get initial timestamps
	originalCreatedAt := source.CreatedAt
	originalUpdatedAt := source.UpdatedAt

	// Update source configuration
	newConfig := map[string]any{
		"topic":          "events",
		"brokers":        []string{"kafka1:9092", "kafka2:9092"},
		"consumer_group": "operion-events",
	}
	err = source.UpdateConfiguration(newConfig)
	require.NoError(t, err)

	// Save updated source
	err = persistence.SaveKafkaSource(source)
	require.NoError(t, err)

	// Retrieve and verify update
	retrievedSource, err := persistence.KafkaSourceByID("test-source")
	require.NoError(t, err)
	require.NotNil(t, retrievedSource)

	assert.Equal(t, "events", retrievedSource.ConnectionDetails.Topic)
	assert.Equal(t, []string{"kafka1:9092", "kafka2:9092"}, retrievedSource.ConnectionDetails.Brokers)
	assert.Equal(t, "operion-events", retrievedSource.ConnectionDetails.ConsumerGroup)
	assertJSONEqual(t, newConfig, retrievedSource.Configuration)

	// Verify timestamps
	assert.Equal(t, originalCreatedAt.Unix(), retrievedSource.CreatedAt.Unix()) // CreatedAt should not change
	assert.True(t, retrievedSource.UpdatedAt.After(originalUpdatedAt))          // UpdatedAt should be newer
}

func TestPostgresPersistence_DeleteKafkaSource(t *testing.T) {
	persistence, _, databaseURL := setupTestDB(t)
	defer persistence.Close()
	defer cleanupDB(t, databaseURL)

	// Create and save test sources
	config1 := map[string]any{"topic": "orders", "brokers": []string{"localhost:9092"}}
	source1, err := models.NewKafkaSource("source-1", config1)
	require.NoError(t, err)

	config2 := map[string]any{"topic": "events", "brokers": []string{"localhost:9092"}}
	source2, err := models.NewKafkaSource("source-2", config2)
	require.NoError(t, err)

	err = persistence.SaveKafkaSource(source1)
	require.NoError(t, err)
	err = persistence.SaveKafkaSource(source2)
	require.NoError(t, err)

	// Verify both sources exist
	sources, err := persistence.KafkaSources()
	require.NoError(t, err)
	assert.Len(t, sources, 2)

	// Delete one source
	err = persistence.DeleteKafkaSource("source-1")
	require.NoError(t, err)

	// Verify source was deleted
	deletedSource, err := persistence.KafkaSourceByID("source-1")
	require.NoError(t, err)
	assert.Nil(t, deletedSource)

	// Verify other source still exists
	remainingSource, err := persistence.KafkaSourceByID("source-2")
	require.NoError(t, err)
	require.NotNil(t, remainingSource)
	assert.Equal(t, "source-2", remainingSource.ID)

	// Verify total count
	sources, err = persistence.KafkaSources()
	require.NoError(t, err)
	assert.Len(t, sources, 1)

	// Delete non-existent source should not error
	err = persistence.DeleteKafkaSource("non-existent")
	assert.NoError(t, err)
}

func TestPostgresPersistence_HealthCheck(t *testing.T) {
	persistence, _, databaseURL := setupTestDB(t)
	defer persistence.Close()
	defer cleanupDB(t, databaseURL)

	// Health check should pass for valid connection
	err := persistence.HealthCheck()
	assert.NoError(t, err)

	// Test after closing connection
	err = persistence.Close()
	require.NoError(t, err)

	// Health check should fail after close
	err = persistence.HealthCheck()
	assert.Error(t, err)
}

func TestPostgresPersistence_PersistenceAcrossReconnections(t *testing.T) {
	_, _, databaseURL := setupTestDB(t)
	defer cleanupDB(t, databaseURL)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Create first persistence instance
	persistence1, err := NewPostgresPersistence(context.Background(), logger, databaseURL)
	require.NoError(t, err)

	// Save sources
	config1 := map[string]any{"topic": "orders", "brokers": []string{"localhost:9092"}}
	source1, err := models.NewKafkaSource("source-1", config1)
	require.NoError(t, err)

	config2 := map[string]any{"topic": "events", "brokers": []string{"localhost:9092"}}
	source2, err := models.NewKafkaSource("source-2", config2)
	require.NoError(t, err)

	err = persistence1.SaveKafkaSource(source1)
	require.NoError(t, err)
	err = persistence1.SaveKafkaSource(source2)
	require.NoError(t, err)

	// Close first instance
	err = persistence1.Close()
	require.NoError(t, err)

	// Create second persistence instance (simulating restart)
	persistence2, err := NewPostgresPersistence(context.Background(), logger, databaseURL)
	require.NoError(t, err)
	defer persistence2.Close()

	// Verify sources were persisted across connections
	sources, err := persistence2.KafkaSources()
	require.NoError(t, err)
	assert.Len(t, sources, 2)

	// Verify source data integrity
	loadedSource1, err := persistence2.KafkaSourceByID("source-1")
	require.NoError(t, err)
	require.NotNil(t, loadedSource1)
	assert.Equal(t, source1.ID, loadedSource1.ID)
	assert.Equal(t, source1.ConnectionDetails.Topic, loadedSource1.ConnectionDetails.Topic)

	loadedSource2, err := persistence2.KafkaSourceByID("source-2")
	require.NoError(t, err)
	require.NotNil(t, loadedSource2)
	assert.Equal(t, source2.ID, loadedSource2.ID)
	assert.Equal(t, source2.ConnectionDetails.Topic, loadedSource2.ConnectionDetails.Topic)
}

func TestPostgresPersistence_JSONSerialization(t *testing.T) {
	persistence, _, databaseURL := setupTestDB(t)
	defer persistence.Close()
	defer cleanupDB(t, databaseURL)

	// Create source with complex JSON schema and configuration
	config := map[string]any{
		"topic":   "orders",
		"brokers": []string{"localhost:9092"},
		"json_schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"order_id": map[string]any{
					"type":    "string",
					"pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
				},
				"items": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"id":    map[string]any{"type": "string"},
							"price": map[string]any{"type": "number"},
						},
					},
				},
			},
			"required": []string{"order_id", "items"},
		},
		"consumer_config": map[string]any{
			"session_timeout":    "30s",
			"heartbeat_interval": "10s",
		},
	}

	source, err := models.NewKafkaSource("complex-source", config)
	require.NoError(t, err)

	// Save and retrieve
	err = persistence.SaveKafkaSource(source)
	require.NoError(t, err)

	retrievedSource, err := persistence.KafkaSourceByID("complex-source")
	require.NoError(t, err)
	require.NotNil(t, retrievedSource)

	// Verify all complex data was preserved
	assertJSONEqual(t, source.JSONSchema, retrievedSource.JSONSchema)
	assertJSONEqual(t, source.Configuration, retrievedSource.Configuration)

	// Verify specific nested values
	jsonSchema := retrievedSource.JSONSchema
	properties := jsonSchema["properties"].(map[string]any)
	orderIdField := properties["order_id"].(map[string]any)
	assert.Equal(t, "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", orderIdField["pattern"])

	consumerConfig := retrievedSource.Configuration["consumer_config"].(map[string]any)
	assert.Equal(t, "30s", consumerConfig["session_timeout"])
	assert.Equal(t, "10s", consumerConfig["heartbeat_interval"])
}

// assertJSONEqual compares two values by JSON marshaling and unmarshaling them
// This handles the type differences that occur during PostgreSQL JSONB storage/retrieval
func assertJSONEqual(t *testing.T, expected, actual any) {
	t.Helper()

	expectedJSON, err := json.Marshal(expected)
	require.NoError(t, err)

	actualJSON, err := json.Marshal(actual)
	require.NoError(t, err)

	assert.JSONEq(t, string(expectedJSON), string(actualJSON))
}
