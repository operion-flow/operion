package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServeFailsWhenDatabaseIsUnreachable(t *testing.T) {
	err := serve(t.Context(), serveOptions{
		databaseURL:  "postgres://operion@127.0.0.1:1/operion?sslmode=disable&connect_timeout=1",
		pollInterval: time.Millisecond,
		logLevel:     "error",
	})

	require.Error(t, err)
}

func TestServeStartsTracingWhenAnOTLPEndpointIsSet(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")

	err := serve(t.Context(), serveOptions{
		databaseURL:  "postgres://operion@127.0.0.1:1/operion?sslmode=disable&connect_timeout=1",
		pollInterval: time.Millisecond,
		logLevel:     "error",
	})

	require.Error(t, err)
}

func TestServeFailsOnAnInvalidNodeTypesFile(t *testing.T) {
	err := serve(t.Context(), serveOptions{
		databaseURL:  "postgres://operion@127.0.0.1:1/operion?sslmode=disable&connect_timeout=1",
		pollInterval: time.Millisecond,
		logLevel:     "error",
		nodeTypes:    writeFile(t, "node-types.json", `{"create_benefit": {"name": "Create benefit"}}`),
	})

	require.ErrorContains(t, err, `node type "create_benefit"`)
}

func TestServeRefusesANegativeExecutionRetention(t *testing.T) {
	err := serve(t.Context(), serveOptions{
		databaseURL:        "postgres://operion@127.0.0.1:1/operion?sslmode=disable&connect_timeout=1",
		pollInterval:       time.Millisecond,
		logLevel:           "error",
		executionRetention: -time.Hour,
	})

	require.ErrorIs(t, err, errNegativeRetention)
}
