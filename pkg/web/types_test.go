package web_test

import (
	"errors"
	"testing"

	"github.com/dukex/operion/pkg/web"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateWorkflowRequest_Validation(t *testing.T) {
	t.Parallel()

	v := validator.New()

	tests := []struct {
		name      string
		request   web.CreateWorkflowRequest
		wantErr   bool
		errFields []string
	}{
		{
			name: "valid request",
			request: web.CreateWorkflowRequest{
				Name:        "Test Workflow",
				Description: "Test Description",
				Owner:       "test-user",
				Variables:   map[string]any{"env": "test"},
				Metadata:    map[string]any{"category": "test"},
			},
			wantErr: false,
		},
		{
			name: "missing name",
			request: web.CreateWorkflowRequest{
				Description: "Test Description",
				Owner:       "test-user",
			},
			wantErr:   true,
			errFields: []string{"Name"},
		},
		{
			name: "name too short",
			request: web.CreateWorkflowRequest{
				Name:        "Te",
				Description: "Test Description",
				Owner:       "test-user",
			},
			wantErr:   true,
			errFields: []string{"Name"},
		},
		{
			name: "missing description",
			request: web.CreateWorkflowRequest{
				Name:  "Test Workflow",
				Owner: "test-user",
			},
			wantErr:   true,
			errFields: []string{"Description"},
		},
		{
			name: "missing owner",
			request: web.CreateWorkflowRequest{
				Name:        "Test Workflow",
				Description: "Test Description",
			},
			wantErr:   true,
			errFields: []string{"Owner"},
		},
		{
			name: "multiple validation errors",
			request: web.CreateWorkflowRequest{
				Name: "Te", // too short
				// missing description and owner
			},
			wantErr:   true,
			errFields: []string{"Name", "Description", "Owner"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := v.Struct(tt.request)

			if tt.wantErr {
				require.Error(t, err)

				var validationErrors validator.ValidationErrors
				if errors.As(err, &validationErrors) {
					// Check that expected fields have validation errors
					errorFields := make(map[string]bool)
					for _, fieldErr := range validationErrors {
						errorFields[fieldErr.Field()] = true
					}

					for _, expectedField := range tt.errFields {
						assert.True(t, errorFields[expectedField], "Expected validation error for field %s", expectedField)
					}
				} else {
					t.Fatalf("Expected validator.ValidationErrors, got %T", err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUpdateWorkflowRequest_Validation(t *testing.T) {
	t.Parallel()

	v := validator.New()

	tests := []struct {
		name      string
		request   web.UpdateWorkflowRequest
		wantErr   bool
		errFields []string
	}{
		{
			name:    "empty request is valid",
			request: web.UpdateWorkflowRequest{},
			wantErr: false,
		},
		{
			name: "valid partial update",
			request: web.UpdateWorkflowRequest{
				Name:      stringPtr("Updated Workflow"),
				Variables: map[string]any{"env": "production"},
			},
			wantErr: false,
		},
		{
			name: "name too short when provided",
			request: web.UpdateWorkflowRequest{
				Name: stringPtr("Te"),
			},
			wantErr:   true,
			errFields: []string{"Name"},
		},
		{
			name: "valid minimum name length",
			request: web.UpdateWorkflowRequest{
				Name: stringPtr("New"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := v.Struct(tt.request)

			if tt.wantErr {
				require.Error(t, err)

				var validationErrors validator.ValidationErrors
				if errors.As(err, &validationErrors) {
					// Check that expected fields have validation errors
					errorFields := make(map[string]bool)
					for _, fieldErr := range validationErrors {
						errorFields[fieldErr.Field()] = true
					}

					for _, expectedField := range tt.errFields {
						assert.True(t, errorFields[expectedField], "Expected validation error for field %s", expectedField)
					}
				} else {
					t.Fatalf("Expected validator.ValidationErrors, got %T", err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper function to get string pointer.
func stringPtr(s string) *string {
	return &s
}
