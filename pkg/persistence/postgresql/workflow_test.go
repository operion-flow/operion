package postgresql

import (
	"context"
	"log/slog"
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkflowRepository_buildListQuery_InvalidSortField tests that invalid sort field returns typed error.
func TestWorkflowRepository_buildListQuery_InvalidSortField(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))
	repo := &WorkflowRepository{
		db:     nil, // Not needed for this test
		logger: logger,
	}

	tests := []struct {
		name    string
		sortBy  string
		wantErr error
	}{
		{
			name:    "invalid sort field should return ErrInvalidSortField",
			sortBy:  "invalid_field",
			wantErr: persistence.ErrInvalidSortField,
		},
		{
			name:    "sql injection attempt should return ErrInvalidSortField",
			sortBy:  "name; DROP TABLE workflows; --",
			wantErr: persistence.ErrInvalidSortField,
		},
		{
			name:    "empty sort field with invalid characters should return ErrInvalidSortField",
			sortBy:  "'; SELECT * FROM users; --",
			wantErr: persistence.ErrInvalidSortField,
		},
		{
			name:    "valid sort field should not return error",
			sortBy:  "name",
			wantErr: nil,
		},
		{
			name:    "created_at sort field should not return error",
			sortBy:  "created_at",
			wantErr: nil,
		},
		{
			name:    "updated_at sort field should not return error",
			sortBy:  "updated_at",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := persistence.ListWorkflowsOptions{
				SortBy:    tt.sortBy,
				SortOrder: "asc",
				Limit:     10,
				Offset:    0,
			}

			_, _, err := repo.buildListQuery(opts)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				assert.True(t, persistence.IsInvalidSortField(err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestWorkflowRepository_saveWorkflowConnections_InvalidPortFormat tests that invalid port format returns typed error.
func TestWorkflowRepository_saveWorkflowConnections_InvalidPortFormat(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))
	repo := &WorkflowRepository{
		db:     nil, // Not needed for this test since we'll return early on port validation
		logger: logger,
	}

	tests := []struct {
		name        string
		sourcePort  string
		targetPort  string
		expectError bool
		expectedErr error
	}{
		{
			name:        "invalid source port format should return ErrInvalidPortFormat",
			sourcePort:  "invalid-format",
			targetPort:  "node2:input",
			expectError: true,
			expectedErr: persistence.ErrInvalidPortFormat,
		},
		{
			name:        "invalid target port format should return ErrInvalidPortFormat",
			sourcePort:  "node1:output",
			targetPort:  "invalid-format",
			expectError: true,
			expectedErr: persistence.ErrInvalidPortFormat,
		},
		{
			name:        "both ports invalid should return ErrInvalidPortFormat for first one",
			sourcePort:  "invalid",
			targetPort:  "also-invalid",
			expectError: true,
			expectedErr: persistence.ErrInvalidPortFormat,
		},
		{
			name:        "empty source port should return ErrInvalidPortFormat",
			sourcePort:  "",
			targetPort:  "node2:input",
			expectError: true,
			expectedErr: persistence.ErrInvalidPortFormat,
		},
		{
			name:        "empty target port should return ErrInvalidPortFormat",
			sourcePort:  "node1:output",
			targetPort:  "",
			expectError: true,
			expectedErr: persistence.ErrInvalidPortFormat,
		},
		{
			name:        "port without colon separator should return ErrInvalidPortFormat",
			sourcePort:  "node1output",
			targetPort:  "node2:input",
			expectError: true,
			expectedErr: persistence.ErrInvalidPortFormat,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflow := &models.Workflow{
				ID: "test-workflow",
				Connections: []*models.Connection{
					{
						ID:         "conn1",
						SourcePort: tt.sourcePort,
						TargetPort: tt.targetPort,
					},
				},
			}

			ctx := context.Background()
			err := repo.saveWorkflowConnections(ctx, nil, workflow) // tx is nil since we expect early return

			if tt.expectError {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.expectedErr)
				assert.True(t, persistence.IsInvalidPortFormat(err))
			} else {
				// For valid port formats, we expect a different error (nil tx panic/error)
				// but NOT the port validation error
				if err != nil {
					assert.False(t, persistence.IsInvalidPortFormat(err))
				}
			}
		})
	}
}

// TestWorkflowRepository_ValidPortFormats tests that valid port formats are parsed correctly.
func TestWorkflowRepository_ValidPortFormats(t *testing.T) {
	tests := []struct {
		name       string
		sourcePort string
		targetPort string
		shouldPass bool
	}{
		{
			name:       "valid port formats with colon separator",
			sourcePort: "node1:output",
			targetPort: "node2:input",
			shouldPass: true,
		},
		{
			name:       "invalid port format without colon",
			sourcePort: "node1output",
			targetPort: "node2:input",
			shouldPass: false,
		},
		{
			name:       "empty port",
			sourcePort: "",
			targetPort: "node2:input",
			shouldPass: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test source port parsing
			_, _, sourceOK := models.ParsePortID(tt.sourcePort)
			// Test target port parsing
			_, _, targetOK := models.ParsePortID(tt.targetPort)

			if tt.shouldPass {
				assert.True(t, sourceOK, "Source port should be valid")
				assert.True(t, targetOK, "Target port should be valid")
			} else {
				// At least one should be invalid
				assert.False(t, sourceOK && targetOK, "At least one port should be invalid")
			}
		})
	}
}

// TestWorkflowRepository_ErrorTypes tests that all error helper functions work correctly.
func TestWorkflowRepository_ErrorTypes(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		isInvalidSort bool
		isInvalidPort bool
		isNotFound    bool
	}{
		{
			name:          "ErrInvalidSortField should be detected by IsInvalidSortField",
			err:           persistence.ErrInvalidSortField,
			isInvalidSort: true,
			isInvalidPort: false,
			isNotFound:    false,
		},
		{
			name:          "ErrInvalidPortFormat should be detected by IsInvalidPortFormat",
			err:           persistence.ErrInvalidPortFormat,
			isInvalidSort: false,
			isInvalidPort: true,
			isNotFound:    false,
		},
		{
			name:          "ErrWorkflowNotFound should be detected by IsWorkflowNotFound",
			err:           persistence.ErrWorkflowNotFound,
			isInvalidSort: false,
			isInvalidPort: false,
			isNotFound:    true,
		},
		{
			name:          "Generic error should not match any helper",
			err:           assert.AnError,
			isInvalidSort: false,
			isInvalidPort: false,
			isNotFound:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.isInvalidSort, persistence.IsInvalidSortField(tt.err))
			assert.Equal(t, tt.isInvalidPort, persistence.IsInvalidPortFormat(tt.err))
			assert.Equal(t, tt.isNotFound, persistence.IsWorkflowNotFound(tt.err))
		})
	}
}

// TestConnectionRepository_SaveConnection_InvalidPortFormat tests typed errors in connection repository.
func TestConnectionRepository_SaveConnection_InvalidPortFormat(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))
	repo := &ConnectionRepository{
		db:     nil, // Not needed for this test since we'll return early on port validation
		logger: logger,
	}

	tests := []struct {
		name       string
		connection *models.Connection
		wantErr    error
	}{
		{
			name: "invalid source port format should return ErrInvalidPortFormat",
			connection: &models.Connection{
				ID:         "conn1",
				SourcePort: "invalid-format",
				TargetPort: "node2:input",
			},
			wantErr: persistence.ErrInvalidPortFormat,
		},
		{
			name: "invalid target port format should return ErrInvalidPortFormat",
			connection: &models.Connection{
				ID:         "conn1",
				SourcePort: "node1:output",
				TargetPort: "invalid-format",
			},
			wantErr: persistence.ErrInvalidPortFormat,
		},
		{
			name: "missing dot separator should return ErrInvalidPortFormat",
			connection: &models.Connection{
				ID:         "conn1",
				SourcePort: "node1output",
				TargetPort: "node2:input",
			},
			wantErr: persistence.ErrInvalidPortFormat,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := repo.SaveConnection(ctx, "workflow-id", tt.connection)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
			assert.True(t, persistence.IsInvalidPortFormat(err))
		})
	}
}
