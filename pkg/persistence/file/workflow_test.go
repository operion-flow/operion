package file

import (
	"context"
	"testing"

	"github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkflowRepository_ListWorkflows_InvalidSortField tests that invalid sort field returns typed error.
func TestWorkflowRepository_ListWorkflows_InvalidSortField(t *testing.T) {
	tempDir := t.TempDir()
	repo := NewWorkflowRepository(tempDir)

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
			name:    "unknown field should return ErrInvalidSortField",
			sortBy:  "unknown_column",
			wantErr: persistence.ErrInvalidSortField,
		},
		{
			name:    "valid sort field name should not return error",
			sortBy:  "name",
			wantErr: nil,
		},
		{
			name:    "valid sort field created_at should not return error",
			sortBy:  "created_at",
			wantErr: nil,
		},
		{
			name:    "valid sort field updated_at should not return error",
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

			ctx := context.Background()
			_, err := repo.ListWorkflows(ctx, opts)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				assert.True(t, persistence.IsInvalidSortField(err))
			} else {
				// For valid sort fields, we might get other errors (like empty directory)
				// but should NOT get invalid sort field error
				if err != nil {
					assert.False(t, persistence.IsInvalidSortField(err))
				}
			}
		})
	}
}

// TestWorkflowRepository_ListWorkflows_ValidSortParameters tests that valid parameters work.
func TestWorkflowRepository_ListWorkflows_ValidSortParameters(t *testing.T) {
	tempDir := t.TempDir()
	repo := NewWorkflowRepository(tempDir)

	validSortFields := []string{"name", "created_at", "updated_at"}
	validSortOrders := []string{"asc", "desc"}

	for _, sortBy := range validSortFields {
		for _, sortOrder := range validSortOrders {
			t.Run(sortBy+"_"+sortOrder, func(t *testing.T) {
				opts := persistence.ListWorkflowsOptions{
					SortBy:    sortBy,
					SortOrder: sortOrder,
					Limit:     10,
					Offset:    0,
				}

				ctx := context.Background()
				_, err := repo.ListWorkflows(ctx, opts)

				// Should not return invalid sort field error
				if err != nil {
					assert.False(t, persistence.IsInvalidSortField(err),
						"Valid sort parameters should not return invalid sort field error")
				}
			})
		}
	}
}

// TestWorkflowRepository_ErrorTypes tests that error helper functions work correctly with file persistence.
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

// TestWorkflowRepository_ListWorkflows_DefaultParameters tests default parameter behavior.
func TestWorkflowRepository_ListWorkflows_DefaultParameters(t *testing.T) {
	tempDir := t.TempDir()
	repo := NewWorkflowRepository(tempDir)

	ctx := context.Background()

	// Test with empty sort parameters (should use defaults)
	opts := persistence.ListWorkflowsOptions{
		SortBy:    "", // Should default to valid field
		SortOrder: "", // Should default to valid order
		Limit:     10,
		Offset:    0,
	}

	_, err := repo.ListWorkflows(ctx, opts)

	// Should not return invalid sort field error with default parameters
	if err != nil {
		assert.False(t, persistence.IsInvalidSortField(err),
			"Default parameters should not return invalid sort field error")
	}
}
