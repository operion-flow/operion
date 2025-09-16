package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// WorkflowRepository handles workflow-related database operations.
type WorkflowRepository struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewWorkflowRepository creates a new workflow repository.
func NewWorkflowRepository(db *sql.DB, logger *slog.Logger) *WorkflowRepository {
	return &WorkflowRepository{db: db, logger: logger}
}

// ListWorkflows returns paginated and filtered workflows with optimized queries to avoid N+1 problem.
func (r *WorkflowRepository) ListWorkflows(ctx context.Context, opts persistence.ListWorkflowsOptions) (*persistence.WorkflowListResult, error) {
	// Build query with security validation
	query, args, err := r.buildListQuery(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	// Execute main query for workflows
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflows: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Intentionally ignore close errors to avoid overriding main error
			_ = closeErr
		}
	}()

	// Scan workflows
	var workflows []*models.Workflow

	for rows.Next() {
		workflow, err := r.scanWorkflowBase(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow: %w", err)
		}

		workflows = append(workflows, workflow)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating workflows: %w", err)
	}

	// Get total count for pagination metadata
	totalCount, err := r.getTotalCount(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get total count: %w", err)
	}

	// Batch load related data if requested (eliminates N+1 queries)
	if len(workflows) > 0 {
		if err := r.loadRelatedData(ctx, workflows, opts); err != nil {
			return nil, fmt.Errorf("failed to load related data: %w", err)
		}
	}

	return &persistence.WorkflowListResult{
		Workflows:   workflows,
		TotalCount:  totalCount,
		HasNextPage: int64(opts.Offset+opts.Limit) < totalCount,
	}, nil
}

// buildListQuery builds a secure, parameterized query for workflow listing.
func (r *WorkflowRepository) buildListQuery(opts persistence.ListWorkflowsOptions) (string, []any, error) {
	baseQuery := `
		SELECT id, name, description, variables, status, metadata, 
			   owner, workflow_group_id, published_at, created_at, updated_at, deleted_at
		FROM workflows
		WHERE deleted_at IS NULL`

	args := []any{}
	argIndex := 1

	// Add filters with parameter binding (secure)
	if opts.OwnerID != "" {
		baseQuery += fmt.Sprintf(" AND owner = $%d", argIndex)

		args = append(args, opts.OwnerID)
		argIndex++
	}

	if opts.Status != nil {
		baseQuery += fmt.Sprintf(" AND status = $%d", argIndex)

		args = append(args, *opts.Status)
		argIndex++
	}

	// Add sorting with allowlist validation (prevent SQL injection)
	orderBy := "created_at DESC" // Default

	if opts.SortBy != "" {
		validSortColumns := map[string]string{
			"created_at": "created_at",
			"updated_at": "updated_at",
			"name":       "name",
		}

		validColumn, exists := validSortColumns[opts.SortBy]
		if !exists {
			return "", nil, persistence.ErrInvalidSortField
		}

		validOrder := "DESC" // Default
		if opts.SortOrder == "asc" {
			validOrder = "ASC"
		}

		orderBy = fmt.Sprintf("%s %s", validColumn, validOrder)
	}

	baseQuery += " ORDER BY " + orderBy

	// Add pagination
	baseQuery += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIndex, argIndex+1)

	args = append(args, opts.Limit, opts.Offset)

	return baseQuery, args, nil
}

// getTotalCount gets the total count of workflows matching the filter criteria.
func (r *WorkflowRepository) getTotalCount(ctx context.Context, opts persistence.ListWorkflowsOptions) (int64, error) {
	baseQuery := `
		SELECT COUNT(*)
		FROM workflows
		WHERE deleted_at IS NULL`

	args := []any{}
	argIndex := 1

	// Apply same filters as main query
	if opts.OwnerID != "" {
		baseQuery += fmt.Sprintf(" AND owner = $%d", argIndex)

		args = append(args, opts.OwnerID)
		argIndex++
	}

	if opts.Status != nil {
		baseQuery += fmt.Sprintf(" AND status = $%d", argIndex)

		args = append(args, *opts.Status)
	}

	var count int64

	err := r.db.QueryRowContext(ctx, baseQuery, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count workflows: %w", err)
	}

	return count, nil
}

// loadRelatedData loads nodes and/or connections for workflows if requested.
func (r *WorkflowRepository) loadRelatedData(ctx context.Context, workflows []*models.Workflow, opts persistence.ListWorkflowsOptions) error {
	workflowIDs := make([]string, len(workflows))
	workflowMap := make(map[string]*models.Workflow)

	for i, w := range workflows {
		workflowIDs[i] = w.ID
		workflowMap[w.ID] = w
	}

	// Batch load nodes if requested
	if opts.IncludeNodes {
		if err := r.loadNodesBatch(ctx, workflowIDs, workflowMap); err != nil {
			return fmt.Errorf("failed to batch load nodes: %w", err)
		}
	}

	// Batch load connections if requested
	if opts.IncludeConnections {
		if err := r.loadConnectionsBatch(ctx, workflowIDs, workflowMap); err != nil {
			return fmt.Errorf("failed to batch load connections: %w", err)
		}
	}

	return nil
}

// loadNodesBatch loads nodes for multiple workflows in a single query (eliminates N+1).
func (r *WorkflowRepository) loadNodesBatch(ctx context.Context, workflowIDs []string, workflowMap map[string]*models.Workflow) error {
	if len(workflowIDs) == 0 {
		return nil
	}

	// Use PostgreSQL array parameter to avoid string concatenation
	query := `
		SELECT workflow_id, id, type, category, name, config, enabled, 
			   position_x, position_y, source_id, provider_id, event_type
		FROM workflow_nodes
		WHERE workflow_id = ANY($1)
		ORDER BY workflow_id, created_at`

	// Convert workflow IDs to PostgreSQL array format
	args := []any{pq.Array(workflowIDs)}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to batch load workflow nodes: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Intentionally ignore close errors to avoid overriding main error
			_ = closeErr
		}
	}()

	// Group nodes by workflow ID
	for rows.Next() {
		var (
			workflowID string
			node       models.WorkflowNode
			configJSON []byte
		)

		err := rows.Scan(
			&workflowID, &node.ID, &node.Type, &node.Category,
			&node.Name, &configJSON, &node.Enabled,
			&node.PositionX, &node.PositionY, &node.SourceID,
			&node.ProviderID, &node.EventType,
		)
		if err != nil {
			return fmt.Errorf("failed to scan workflow node: %w", err)
		}

		// Unmarshal config JSON
		if len(configJSON) > 0 {
			err = json.Unmarshal(configJSON, &node.Config)
			if err != nil {
				return fmt.Errorf("failed to unmarshal node config: %w", err)
			}
		}

		// Add node to corresponding workflow
		if workflow, exists := workflowMap[workflowID]; exists {
			if workflow.Nodes == nil {
				workflow.Nodes = []*models.WorkflowNode{}
			}

			workflow.Nodes = append(workflow.Nodes, &node)
		}
	}

	return rows.Err()
}

// loadConnectionsBatch loads connections for multiple workflows in a single query (eliminates N+1).
func (r *WorkflowRepository) loadConnectionsBatch(ctx context.Context, workflowIDs []string, workflowMap map[string]*models.Workflow) error {
	if len(workflowIDs) == 0 {
		return nil
	}

	// Use PostgreSQL array parameter to avoid string concatenation
	query := `
		SELECT workflow_id, id, source_node_id, source_port, target_node_id, target_port
		FROM workflow_connections
		WHERE workflow_id = ANY($1)
		ORDER BY workflow_id, created_at`

	// Convert workflow IDs to PostgreSQL array format
	args := []any{pq.Array(workflowIDs)}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to batch load workflow connections: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Intentionally ignore close errors to avoid overriding main error
			_ = closeErr
		}
	}()

	// Group connections by workflow ID
	for rows.Next() {
		var (
			workflowID                                         string
			connection                                         models.Connection
			sourceNodeID, sourcePort, targetNodeID, targetPort string
		)

		err := rows.Scan(
			&workflowID, &connection.ID,
			&sourceNodeID, &sourcePort,
			&targetNodeID, &targetPort,
		)
		if err != nil {
			return fmt.Errorf("failed to scan workflow connection: %w", err)
		}

		// Convert database format to Connection struct format
		connection.SourcePort = sourceNodeID + ":" + sourcePort
		connection.TargetPort = targetNodeID + ":" + targetPort

		// Add connection to corresponding workflow
		if workflow, exists := workflowMap[workflowID]; exists {
			if workflow.Connections == nil {
				workflow.Connections = []*models.Connection{}
			}

			workflow.Connections = append(workflow.Connections, &connection)
		}
	}

	return rows.Err()
}

func (r *WorkflowRepository) GetByID(ctx context.Context, id string) (*models.Workflow, error) {
	query := `
		SELECT
			id
		  , name
		  , description
		  , variables
		  , status
		  , metadata
		  , owner
		  , workflow_group_id
		  , published_at
		  , created_at
		  , updated_at
		  , deleted_at
		FROM workflows
		WHERE id = $1 AND deleted_at IS NULL
	`

	row := r.db.QueryRowContext(ctx, query, id)

	workflow, err := r.scanWorkflowBase(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to scan workflow: %w", err)
	}

	if err := r.loadWorkflowNodes(ctx, workflow); err != nil {
		return nil, fmt.Errorf("failed to load workflow triggers and nodes: %w", err)
	}

	return workflow, nil
}

// Save saves a workflow to the database.
func (r *WorkflowRepository) Save(ctx context.Context, workflow *models.Workflow) error {
	now := time.Now().UTC()

	if workflow.CreatedAt.IsZero() {
		workflow.CreatedAt = now
	}

	workflow.UpdatedAt = now

	if workflow.ID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("failed to generate workflow ID: %w", err)
		}

		workflow.ID = id.String()
	}

	// Set workflow_group_id = id for new workflows (when WorkflowGroupID is empty)
	if workflow.WorkflowGroupID == "" {
		workflow.WorkflowGroupID = workflow.ID
	}

	// Start transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Convert complex fields to JSON
	variablesJSON, err := json.Marshal(workflow.Variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables: %w", err)
	}

	metadataJSON, err := json.Marshal(workflow.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Save workflow base data
	workflowQuery := `
		INSERT INTO workflows (id, name, description,
variables, status, metadata, owner, workflow_group_id, published_at, created_at, updated_at, deleted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			description = EXCLUDED.description,
			variables = EXCLUDED.variables,
			status = EXCLUDED.status,
			metadata = EXCLUDED.metadata,
			owner = EXCLUDED.owner,
			workflow_group_id = EXCLUDED.workflow_group_id,
			published_at = EXCLUDED.published_at,
			updated_at = EXCLUDED.updated_at,
			deleted_at = EXCLUDED.deleted_at
	`

	// Convert empty UUID strings to NULL for PostgreSQL compatibility
	var workflowGroupIDParam any
	if workflow.WorkflowGroupID == "" {
		workflowGroupIDParam = nil
	} else {
		workflowGroupIDParam = workflow.WorkflowGroupID
	}

	_, err = tx.ExecContext(ctx, workflowQuery,
		workflow.ID,
		workflow.Name,
		workflow.Description,
		variablesJSON,
		workflow.Status,
		metadataJSON,
		workflow.Owner,
		workflowGroupIDParam,
		workflow.PublishedAt,
		workflow.CreatedAt,
		workflow.UpdatedAt,
		workflow.DeletedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to save workflow base: %w", err)
	}

	// Delete existing nodes and connections (for updates)
	_, err = tx.ExecContext(ctx, "DELETE FROM workflow_connections WHERE workflow_id = $1", workflow.ID)
	if err != nil {
		return fmt.Errorf("failed to delete existing connections: %w", err)
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM workflow_nodes WHERE workflow_id = $1", workflow.ID)
	if err != nil {
		return fmt.Errorf("failed to delete existing nodes: %w", err)
	}

	// Save nodes and connections
	if err := r.saveWorkflowNodes(ctx, tx, workflow); err != nil {
		return fmt.Errorf("failed to save workflow nodes: %w", err)
	}

	if err := r.saveWorkflowConnections(ctx, tx, workflow); err != nil {
		return fmt.Errorf("failed to save workflow connections: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Delete soft deletes a workflow by setting deleted_at timestamp.
func (r *WorkflowRepository) Delete(ctx context.Context, id string) error {
	query := `UPDATE workflows SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete workflow: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Workflow doesn't exist or already deleted - this is not an error
		return nil
	}

	return nil
}

// GetCurrentWorkflow returns the current version (published if exists, otherwise draft).
func (r *WorkflowRepository) GetCurrentWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	// Try published first, then draft
	query := `
		SELECT
			id
		  , name
		  , description
		  , variables
		  , status
		  , metadata
		  , owner
		  , workflow_group_id
		  , published_at
		  , created_at
		  , updated_at
		  , deleted_at
		FROM workflows 
		WHERE workflow_group_id = $1 AND status IN ('published', 'draft') AND deleted_at IS NULL 
		ORDER BY CASE WHEN status = 'published' THEN 0 ELSE 1 END
		LIMIT 1
	`

	row := r.db.QueryRowContext(ctx, query, workflowGroupID)

	workflow, err := r.scanWorkflowBase(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to scan workflow: %w", err)
	}

	if err := r.loadWorkflowNodes(ctx, workflow); err != nil {
		return nil, fmt.Errorf("failed to load workflow nodes: %w", err)
	}

	return workflow, nil
}

// GetDraftWorkflow returns the draft version of a workflow group.
func (r *WorkflowRepository) GetDraftWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	query := `
		SELECT
			id
		  , name
		  , description
		  , variables
		  , status
		  , metadata
		  , owner
		  , workflow_group_id
		  , published_at
		  , created_at
		  , updated_at
		  , deleted_at
		FROM workflows
		WHERE workflow_group_id = $1 AND status = 'draft' AND deleted_at IS NULL
		ORDER BY created_at DESC
		LIMIT 1
	`

	row := r.db.QueryRowContext(ctx, query, workflowGroupID)

	workflow, err := r.scanWorkflowBase(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to scan workflow: %w", err)
	}

	if err := r.loadWorkflowNodes(ctx, workflow); err != nil {
		return nil, fmt.Errorf("failed to load workflow nodes: %w", err)
	}

	return workflow, nil
}

// GetPublishedWorkflow returns the published version of a workflow group.
func (r *WorkflowRepository) GetPublishedWorkflow(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	query := `
		SELECT
			id
		  , name
		  , description
		  , variables
		  , status
		  , metadata
		  , owner
		  , workflow_group_id
		  , published_at
		  , created_at
		  , updated_at
		  , deleted_at
		FROM workflows
		WHERE workflow_group_id = $1 AND status = 'published' AND deleted_at IS NULL
		ORDER BY created_at DESC
		LIMIT 1
	`

	row := r.db.QueryRowContext(ctx, query, workflowGroupID)

	workflow, err := r.scanWorkflowBase(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to scan workflow: %w", err)
	}

	if err := r.loadWorkflowNodes(ctx, workflow); err != nil {
		return nil, fmt.Errorf("failed to load workflow nodes: %w", err)
	}

	return workflow, nil
}

// PublishWorkflow handles the publish operation.
func (r *WorkflowRepository) PublishWorkflow(ctx context.Context, workflowID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback() // Ignore rollback errors in defer
	}()

	// Get the workflow being published
	workflow, err := r.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to get workflow: %w", err)
	}

	if workflow == nil {
		return fmt.Errorf("workflow not found: %s", workflowID)
	}

	// Set all other workflows in group to unpublished
	_, err = tx.ExecContext(ctx,
		"UPDATE workflows SET status = 'unpublished' WHERE workflow_group_id = $1 AND status = 'published'",
		workflow.WorkflowGroupID)
	if err != nil {
		return fmt.Errorf("failed to unpublish existing workflows: %w", err)
	}

	// Set current workflow to published (set published_at if not already set)
	_, err = tx.ExecContext(ctx,
		"UPDATE workflows SET status = 'published', updated_at = NOW(), published_at = COALESCE(published_at, NOW()) WHERE id = $1",
		workflowID)
	if err != nil {
		return fmt.Errorf("failed to publish workflow: %w", err)
	}

	return tx.Commit()
}

// CreateDraftFromPublished creates a draft copy from published version.
func (r *WorkflowRepository) CreateDraftFromPublished(ctx context.Context, workflowGroupID string) (*models.Workflow, error) {
	// Check if draft already exists
	existingDraft, err := r.GetDraftWorkflow(ctx, workflowGroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing draft: %w", err)
	}

	if existingDraft != nil {
		return existingDraft, nil
	}

	// Get published workflow
	publishedWorkflow, err := r.GetPublishedWorkflow(ctx, workflowGroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get published workflow: %w", err)
	}

	if publishedWorkflow == nil {
		return nil, persistence.NewWorkflowGroupError("CreateDraftFromPublished", workflowGroupID, persistence.ErrPublishedWorkflowNotFound)
	}

	// Create draft copy
	draftWorkflow := *publishedWorkflow

	// Generate new ID for draft
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("failed to generate workflow ID: %w", err)
	}

	draftWorkflow.ID = id.String()

	// Set as draft
	draftWorkflow.Status = models.WorkflowStatusDraft
	draftWorkflow.CreatedAt = time.Now().UTC()
	draftWorkflow.UpdatedAt = time.Now().UTC()
	draftWorkflow.PublishedAt = nil

	// Save the draft
	if err := r.Save(ctx, &draftWorkflow); err != nil {
		return nil, fmt.Errorf("failed to save draft workflow: %w", err)
	}

	return &draftWorkflow, nil
}

func (r *WorkflowRepository) loadWorkflowNodes(ctx context.Context, workflow *models.Workflow) error {
	// Triggers are now part of nodes, so we only load nodes with all trigger fields

	// Load nodes with trigger fields
	nodesQuery := `
		SELECT id, type, category, name, config, enabled, position_x, position_y, source_id, provider_id, event_type
		FROM workflow_nodes
		WHERE workflow_id = $1
		ORDER BY created_at
	`

	rows, err := r.db.QueryContext(ctx, nodesQuery, workflow.ID)
	if err != nil {
		return fmt.Errorf("failed to query workflow nodes: %w", err)
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			r.logger.Error("failed to close rows", "error", err)
		}
	}()

	var nodes []*models.WorkflowNode

	for rows.Next() {
		var (
			node       models.WorkflowNode
			configJSON []byte
		)

		err := rows.Scan(
			&node.ID,
			&node.Type,
			&node.Category,
			&node.Name,
			&configJSON,
			&node.Enabled,
			&node.PositionX,
			&node.PositionY,
			&node.SourceID,
			&node.ProviderID,
			&node.EventType,
		)
		if err != nil {
			return fmt.Errorf("failed to scan node: %w", err)
		}

		if configJSON != nil {
			err := json.Unmarshal(configJSON, &node.Config)
			if err != nil {
				return fmt.Errorf("failed to unmarshal node configuration: %w", err)
			}
		}

		nodes = append(nodes, &node)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating nodes: %w", err)
	}

	workflow.Nodes = nodes

	// Load connections
	connectionsQuery := `
		SELECT id, source_node_id, source_port, target_node_id, target_port
		FROM workflow_connections
		WHERE workflow_id = $1
		ORDER BY created_at
	`

	rows, err = r.db.QueryContext(ctx, connectionsQuery, workflow.ID)
	if err != nil {
		return fmt.Errorf("failed to query workflow connections: %w", err)
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			r.logger.Error("failed to close rows", "error", err)
		}
	}()

	var connections []*models.Connection

	for rows.Next() {
		var (
			connection                                         models.Connection
			sourceNodeID, sourcePort, targetNodeID, targetPort string
		)

		err := rows.Scan(
			&connection.ID,
			&sourceNodeID,
			&sourcePort,
			&targetNodeID,
			&targetPort,
		)
		if err != nil {
			return fmt.Errorf("failed to scan connection: %w", err)
		}

		// Convert old database format to new Connection struct format
		connection.SourcePort = sourceNodeID + ":" + sourcePort
		connection.TargetPort = targetNodeID + ":" + targetPort

		connections = append(connections, &connection)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating connections: %w", err)
	}

	workflow.Connections = connections

	return nil
}

// Triggers are now saved as part of nodes - no separate trigger saving function needed

// saveWorkflowNodes saves nodes for a workflow.
func (r *WorkflowRepository) saveWorkflowNodes(ctx context.Context, tx *sql.Tx, workflow *models.Workflow) error {
	for _, node := range workflow.Nodes {
		configJSON, err := json.Marshal(node.Config)
		if err != nil {
			return fmt.Errorf("failed to marshal node configuration: %w", err)
		}

		query := `
			INSERT INTO workflow_nodes (id, workflow_id, type, category, name, config, enabled, position_x, position_y, source_id, provider_id, event_type)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`

		_, err = tx.ExecContext(ctx, query,
			node.ID,
			workflow.ID,
			node.Type,
			node.Category,
			node.Name,
			configJSON,
			node.Enabled,
			node.PositionX,
			node.PositionY,
			node.SourceID,
			node.ProviderID,
			node.EventType,
		)
		if err != nil {
			return fmt.Errorf("failed to save node: %w", err)
		}
	}

	return nil
}

// saveWorkflowConnections saves connections for a workflow.
func (r *WorkflowRepository) saveWorkflowConnections(ctx context.Context, tx *sql.Tx, workflow *models.Workflow) error {
	for _, connection := range workflow.Connections {
		// Parse port IDs to extract node IDs and port names
		sourceNodeID, sourcePortName, sourceOK := models.ParsePortID(connection.SourcePort)
		if !sourceOK {
			return persistence.ErrInvalidPortFormat
		}

		targetNodeID, targetPortName, targetOK := models.ParsePortID(connection.TargetPort)
		if !targetOK {
			return persistence.ErrInvalidPortFormat
		}

		query := `
			INSERT INTO workflow_connections (id, workflow_id, source_node_id, source_port, target_node_id, target_port)
			VALUES ($1, $2, $3, $4, $5, $6)
		`

		_, err := tx.ExecContext(ctx, query,
			connection.ID,
			workflow.ID,
			sourceNodeID,
			sourcePortName,
			targetNodeID,
			targetPortName,
		)
		if err != nil {
			return fmt.Errorf("failed to save connection: %w", err)
		}
	}

	return nil
}

func (r *WorkflowRepository) scanWorkflowBase(scanner interface {
	Scan(dest ...any) error
}) (*models.Workflow, error) {
	var (
		workflow                    models.Workflow
		variablesJSON, metadataJSON []byte
		workflowGroupID             sql.NullString
	)

	err := scanner.Scan(
		&workflow.ID,
		&workflow.Name,
		&workflow.Description,
		&variablesJSON,
		&workflow.Status,
		&metadataJSON,
		&workflow.Owner,
		&workflowGroupID,
		&workflow.PublishedAt,
		&workflow.CreatedAt,
		&workflow.UpdatedAt,
		&workflow.DeletedAt,
	)
	if err != nil {
		return nil, err
	}

	// Convert nullable strings to regular strings
	if workflowGroupID.Valid {
		workflow.WorkflowGroupID = workflowGroupID.String
	}

	// Unmarshal JSON fields
	if variablesJSON != nil {
		err := json.Unmarshal(variablesJSON, &workflow.Variables)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal variables: %w", err)
		}
	}

	if metadataJSON != nil {
		err := json.Unmarshal(metadataJSON, &workflow.Metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &workflow, nil
}
