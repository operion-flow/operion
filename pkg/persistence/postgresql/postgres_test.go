package postgresql_test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var postgresContainer *postgres.PostgresContainer

func dropDb(ctx context.Context, t *testing.T, databaseURL string) {
	t.Helper()

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	// Drop tables in reverse dependency order (children first, parents last)
	for _, table := range []string{"input_coordination_states", "execution_contexts", "workflow_connections", "workflow_nodes", "workflows", "schema_migrations"} {
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table+" CASCADE")
		require.NoError(t, err)
	}

	err = db.Close()
	require.NoError(t, err)
}

func setupTestDB(t *testing.T) (*postgresql.Persistence, context.Context, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	if postgresContainer == nil || !postgresContainer.IsRunning() {
		var err error

		postgresContainer, err = postgres.Run(ctx,
			"postgres:16-alpine",
			postgres.WithDatabase("operion_test"),
			postgres.WithUsername("operion"),
			postgres.WithPassword("operion"),
			postgres.BasicWaitStrategies(),
		)
		require.NoError(t, err)
	}

	databaseURL, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dropDb(ctx, t, databaseURL)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	persistence, err := postgresql.NewPersistence(ctx, logger, databaseURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		dropDb(ctx, t, databaseURL)

		err = persistence.Close(ctx)
		require.NoError(t, err)

		cancel()
	})

	return persistence, ctx, databaseURL
}

func TestNewPersistence_Migrations(t *testing.T) {
	_, ctx, databaseURL := setupTestDB(t)

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// Verify tables were created
	var exists bool

	err = db.QueryRowContext(ctx, `SELECT EXISTS (SELECT FROM
information_schema.tables WHERE table_name = 'workflows')`).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists, "workflows table should exist")

	err = db.QueryRowContext(ctx, `SELECT EXISTS (SELECT FROM
information_schema.tables WHERE table_name = 'schema_migrations')`).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists, "schema_migrations table should exist")

	var version int

	err = db.QueryRowContext(ctx, "SELECT version FROM schema_migrations WHERE version = 1").Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, 1, version)
}

func TestNewPersistence_HealthCheck(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	err := p.HealthCheck(ctx)
	assert.NoError(t, err)
}

func TestNewPersistence_SaveAndRetrieveWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	sourceID := uuid.New().String()
	workflow := &models.Workflow{
		Name:        "Test Workflow",
		Description: "A test workflow",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "trigger1",
				Type:       "trigger:scheduler",
				Category:   models.CategoryTypeTrigger,
				Name:       "Daily Schedule",
				Config:     map[string]any{"cron": "0 0 * * *"},
				SourceID:   &sourceID,
				ProviderID: &[]string{"scheduler"}[0],
				EventType:  &[]string{"schedule_due"}[0],
				Enabled:    true,
			},
			{
				ID:       "step1",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Name:     "Log Message",
				Config:   map[string]any{"message": "Hello World"},
				Enabled:  true,
			},
		},
		Connections: []*models.Connection{},
		Variables: map[string]any{
			"test_var": "test_value",
		},
		Status: models.WorkflowStatusPublished,
		Metadata: map[string]any{
			"created_by": "test",
		},
		Owner: "test-user",
	}

	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)
	assert.False(t, workflow.CreatedAt.IsZero())
	assert.False(t, workflow.UpdatedAt.IsZero())

	// Test retrieving workflow by ID
	retrieved, err := p.WorkflowRepository().GetByID(ctx, workflow.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, workflow.ID, retrieved.ID)
	assert.Equal(t, workflow.Name, retrieved.Name)
	assert.Equal(t, workflow.Description, retrieved.Description)
	assert.Equal(t, workflow.Status, retrieved.Status)
	assert.Equal(t, workflow.Owner, retrieved.Owner)

	// Count trigger nodes in retrieved workflow
	triggerNodeCount := 0
	expectedTriggerNodeCount := 0

	for _, node := range retrieved.Nodes {
		if node.IsTriggerNode() {
			triggerNodeCount++
		}
	}

	for _, node := range workflow.Nodes {
		if node.IsTriggerNode() {
			expectedTriggerNodeCount++
		}
	}

	assert.Equal(t, expectedTriggerNodeCount, triggerNodeCount, "Should have same number of trigger nodes")
	assert.Len(t, retrieved.Nodes, len(workflow.Nodes))
	assert.Equal(t, workflow.Variables["test_var"], retrieved.Variables["test_var"])
	assert.Equal(t, workflow.Metadata["created_by"], retrieved.Metadata["created_by"])

	// Test retrieving non-existent workflow
	notFound, err := p.WorkflowRepository().GetByID(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, notFound)
}

func TestNewPersistence_UpdateWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	sourceID := uuid.New().String()
	workflow := &models.Workflow{
		Name:        "Test Workflow",
		Description: "A test workflow",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "trigger1",
				Type:       "trigger:scheduler",
				Category:   models.CategoryTypeTrigger,
				Name:       "Daily Schedule",
				Config:     map[string]any{"cron": "0 0 * * *"},
				SourceID:   &sourceID,
				ProviderID: &[]string{"scheduler"}[0],
				EventType:  &[]string{"schedule_due"}[0],
				Enabled:    true,
			},
			{
				ID:       "step1",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Name:     "Log Message",
				Config:   map[string]any{"message": "Hello World"},
				Enabled:  true,
			},
		},
		Connections: []*models.Connection{},
		Status:      models.WorkflowStatusPublished,
		Owner:       "test-user",
	}

	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	initialUpdatedAt := workflow.UpdatedAt

	// Wait a moment to ensure different timestamp
	time.Sleep(10 * time.Millisecond)

	// Update workflow
	workflow.Name = "Updated Test Workflow"
	workflow.Description = "An updated test workflow"
	workflow.Status = models.WorkflowStatusDraft

	err = p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	// Verify update
	retrieved, err := p.WorkflowRepository().GetByID(ctx, workflow.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, "Updated Test Workflow", retrieved.Name)
	assert.Equal(t, "An updated test workflow", retrieved.Description)
	assert.Equal(t, models.WorkflowStatusDraft, retrieved.Status)
	assert.True(t, retrieved.UpdatedAt.After(initialUpdatedAt))
}

func TestNewPersistence_ListWorkflows(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	sourceID3 := uuid.New().String()
	sourceID4 := uuid.New().String()

	workflows := []*models.Workflow{
		{
			Name:        "Test Workflow 3",
			Description: "Description 3",
			Nodes: []*models.WorkflowNode{
				{
					ID:         "trigger1",
					Type:       "trigger:scheduler",
					Category:   models.CategoryTypeTrigger,
					Name:       "Daily Schedule",
					Config:     map[string]any{"cron": "0 0 * * *"},
					SourceID:   &sourceID3,
					ProviderID: &[]string{"scheduler"}[0],
					EventType:  &[]string{"schedule_due"}[0],
					Enabled:    true,
				},
				{
					ID:       "step1",
					Type:     "log",
					Category: models.CategoryTypeAction,
					Name:     "Log Message 3",
					Config: map[string]any{
						"message": "Hello 3",
					},
					Enabled: true,
				},
			},
			Connections: []*models.Connection{},
			Status:      models.WorkflowStatusPublished,
			Owner:       "test-user",
		},
		{
			Name:        "Test Workflow 4",
			Description: "Description 4",
			Nodes: []*models.WorkflowNode{
				{
					ID:         "trigger1",
					Type:       "trigger:scheduler",
					Category:   models.CategoryTypeTrigger,
					Name:       "Daily Schedule",
					Config:     map[string]any{"cron": "0 0 * * *"},
					SourceID:   &sourceID4,
					ProviderID: &[]string{"scheduler"}[0],
					EventType:  &[]string{"schedule_due"}[0],
					Enabled:    true,
				},
				{
					ID:       "step1",
					Type:     "log",
					Category: models.CategoryTypeAction,
					Name:     "Log Message 4",
					Config: map[string]any{
						"message": "Hello 4",
					},
					Enabled: true,
				},
			},
			Connections: []*models.Connection{},
			Status:      models.WorkflowStatusDraft,
			Owner:       "test-user",
		},
	}

	for _, workflow := range workflows {
		err := p.WorkflowRepository().Save(ctx, workflow)
		require.NoError(t, err)
	}

	result, err := p.WorkflowRepository().ListWorkflows(ctx, persistence.ListWorkflowsOptions{
		Limit: 100,
	})
	require.NoError(t, err)

	assert.Len(t, result.Workflows, len(workflows))
}

func TestNewPersistence_DeleteWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	sourceID := uuid.New().String()

	workflow := &models.Workflow{
		Name:        "Test Workflow to Delete",
		Description: "A test workflow for deletion",
		Nodes: []*models.WorkflowNode{
			{
				ID:         "trigger1",
				Type:       "trigger:scheduler",
				Category:   models.CategoryTypeTrigger,
				Name:       "Daily Schedule",
				Config:     map[string]any{"cron": "0 0 * * *"},
				SourceID:   &sourceID,
				ProviderID: &[]string{"scheduler"}[0],
				EventType:  &[]string{"schedule_due"}[0],
				Enabled:    true,
			},
			{
				ID:       "step1",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Name:     "Log Delete Message",
				Config: map[string]any{
					"message": "Hello Delete",
				},
				Enabled: true,
			},
		},
		Connections: []*models.Connection{},
		Status:      models.WorkflowStatusPublished,
		Owner:       "test-user",
	}

	// Save workflow
	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	// Verify it exists
	retrieved, err := p.WorkflowRepository().GetByID(ctx, workflow.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	// Delete workflow
	err = p.WorkflowRepository().Delete(ctx, workflow.ID)
	require.NoError(t, err)

	// Verify it's gone (soft delete)
	deleted, err := p.WorkflowRepository().GetByID(ctx, workflow.ID)
	require.NoError(t, err)
	assert.Nil(t, deleted)

	// Delete non-existent workflow (should not error)
	err = p.WorkflowRepository().Delete(ctx, uuid.NewString())
	assert.NoError(t, err)
}

func TestNewPersistence_ComplexWorkflow(t *testing.T) {
	p, ctx, _ := setupTestDB(t)

	schedulerSourceID := uuid.New().String()
	webhookSourceID := uuid.New().String()

	workflow := &models.Workflow{
		Name:        "Complex Test Workflow",
		Description: "A complex workflow with multiple triggers and steps",
		Nodes: []*models.WorkflowNode{
			{
				ID:       "scheduler_trigger",
				Type:     "trigger:scheduler",
				Category: models.CategoryTypeTrigger,
				Name:     "Daily Schedule",
				Config: map[string]any{
					"cron":        "0 0 * * *",
					"workflow_id": "test-complex-workflow",
					"enabled":     true,
				},
				SourceID:   &schedulerSourceID,
				ProviderID: &[]string{"scheduler"}[0],
				EventType:  &[]string{"schedule_due"}[0],
				Enabled:    true,
			},
			{
				ID:       "webhook_trigger",
				Type:     "trigger:webhook",
				Category: models.CategoryTypeTrigger,
				Name:     "Webhook Handler",
				Config: map[string]any{
					"path":        "/webhook/test",
					"method":      "POST",
					"workflow_id": "test-complex-workflow",
				},
				SourceID:   &webhookSourceID,
				ProviderID: &[]string{"webhook"}[0],
				EventType:  &[]string{"webhook_received"}[0],
				Enabled:    true,
			},
			{
				ID:       "fetch_data",
				Type:     "http_request",
				Category: models.CategoryTypeAction,
				Name:     "Fetch Data",
				Config: map[string]any{
					"url":    "https://api.example.com/data",
					"method": "GET",
					"headers": map[string]any{
						"Authorization": "Bearer token",
					},
				},
				Enabled: true,
			},
			{
				ID:       "transform_data",
				Type:     "transform",
				Category: models.CategoryTypeAction,
				Name:     "Transform Data",
				Config: map[string]any{
					"expression": "$.data",
					"input":      "{{steps.fetch_data.body}}",
				},
				Enabled: true,
			},
			{
				ID:       "log_result",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Name:     "Log Result",
				Config: map[string]any{
					"message": "Processing completed: {{steps.transform_data.result}}",
					"level":   "info",
				},
				Enabled: true,
			},
			{
				ID:       "error_handler",
				Type:     "log",
				Category: models.CategoryTypeAction,
				Name:     "Error Handler",
				Config: map[string]any{
					"message": "Error occurred: {{steps.fetch_data.error}}",
					"level":   "error",
				},
				Enabled: true,
			},
		},
		Connections: []*models.Connection{
			{
				ID:         "conn1",
				SourcePort: "fetch_data:success",
				TargetPort: "transform_data:input",
			},
			{
				ID:         "conn2",
				SourcePort: "fetch_data:failure",
				TargetPort: "error_handler:input",
			},
			{
				ID:         "conn3",
				SourcePort: "transform_data:success",
				TargetPort: "log_result:input",
			},
		},
		Variables: map[string]any{
			"api_base_url": "https://api.example.com",
			"timeout":      30,
			"retry_count":  3,
		},
		Status: models.WorkflowStatusPublished,
		Metadata: map[string]any{
			"version":     "1.0.0",
			"environment": "test",
			"tags":        []string{"test", "complex", "api"},
		},
		Owner: "test-user",
	}

	err := p.WorkflowRepository().Save(ctx, workflow)
	require.NoError(t, err)

	// Retrieve and verify
	retrieved, err := p.WorkflowRepository().GetByID(ctx, workflow.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, workflow.ID, retrieved.ID)
	assert.Equal(t, workflow.Name, retrieved.Name)
	assert.Len(t, retrieved.Nodes, len(workflow.Nodes))

	// Verify trigger nodes
	triggerNodes := make([]*models.WorkflowNode, 0)
	for _, node := range retrieved.Nodes {
		if node.Category == models.CategoryTypeTrigger {
			triggerNodes = append(triggerNodes, node)
		}
	}

	assert.Len(t, triggerNodes, 2)

	for _, trigger := range triggerNodes {
		switch *trigger.ProviderID {
		case "scheduler":
			assert.Equal(t, "0 0 * * *", trigger.Config["cron"])
			assert.Equal(t, true, trigger.Config["enabled"])
			assert.Equal(t, "trigger:scheduler", trigger.Type)
			assert.Equal(t, "schedule_due", *trigger.EventType)
		case "webhook":
			assert.Equal(t, "/webhook/test", trigger.Config["path"])
			assert.Equal(t, "POST", trigger.Config["method"])
			assert.Equal(t, "trigger:webhook", trigger.Type)
			assert.Equal(t, "webhook_received", *trigger.EventType)
		}
	}

	assert.Len(t, retrieved.Nodes, len(workflow.Nodes))

	for _, node := range retrieved.Nodes {
		switch node.ID {
		case "scheduler_trigger":
			assert.Equal(t, "trigger:scheduler", node.Type)
			assert.Equal(t, models.CategoryTypeTrigger, node.Category)
			assert.Equal(t, "0 0 * * *", node.Config["cron"])
			assert.Equal(t, "scheduler", *node.ProviderID)
			assert.Equal(t, "schedule_due", *node.EventType)
		case "webhook_trigger":
			assert.Equal(t, "trigger:webhook", node.Type)
			assert.Equal(t, models.CategoryTypeTrigger, node.Category)
			assert.Equal(t, "/webhook/test", node.Config["path"])
			assert.Equal(t, "webhook", *node.ProviderID)
			assert.Equal(t, "webhook_received", *node.EventType)
		case "fetch_data":
			assert.Equal(t, "http_request", node.Type)
			assert.Equal(t, models.CategoryTypeAction, node.Category)
			assert.Equal(t, "https://api.example.com/data", node.Config["url"])
			assert.Equal(t, "GET", node.Config["method"])
		case "transform_data":
			assert.Equal(t, "transform", node.Type)
			assert.Equal(t, models.CategoryTypeAction, node.Category)
			assert.Equal(t, "$.data", node.Config["expression"])
			assert.Equal(t, "{{steps.fetch_data.body}}", node.Config["input"])
		case "log_result":
			assert.Equal(t, "log", node.Type)
			assert.Equal(t, models.CategoryTypeAction, node.Category)
			assert.Equal(t, "Processing completed: {{steps.transform_data.result}}", node.Config["message"])
		case "error_handler":
			assert.Equal(t, "log", node.Type)
			assert.Equal(t, models.CategoryTypeAction, node.Category)
			assert.Equal(t, "Error occurred: {{steps.fetch_data.error}}", node.Config["message"])
		}
	}

	// Verify connections
	assert.Len(t, retrieved.Connections, len(workflow.Connections))

	for _, conn := range retrieved.Connections {
		switch conn.SourcePort {
		case "fetch_data:success":
			assert.Equal(t, "transform_data:input", conn.TargetPort)
		case "fetch_data:failure":
			assert.Equal(t, "error_handler:input", conn.TargetPort)
		case "transform_data:success":
			assert.Equal(t, "log_result:input", conn.TargetPort)
		}
	}

	// Verify variables and metadata
	assert.Equal(t, "https://api.example.com", retrieved.Variables["api_base_url"])
	assert.Equal(t, float64(30), retrieved.Variables["timeout"]) // JSON unmarshals numbers as float64
	assert.Equal(t, "1.0.0", retrieved.Metadata["version"])
}
