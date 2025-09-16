package web_test

import (
	"errors"
	"testing"

	"github.com/dukex/operion/pkg/models"
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

func TestTransformNodeResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		node     *models.WorkflowNode
		validate func(t *testing.T, response web.NodeResponse)
	}{
		{
			name: "action node excludes trigger fields",
			node: &models.WorkflowNode{
				ID:         "action-1",
				Type:       "log",
				Category:   models.CategoryTypeAction,
				Name:       "Log Action",
				Config:     map[string]any{"message": "test"},
				Enabled:    true,
				PositionX:  100,
				PositionY:  200,
				ProviderID: stringPtr("should-not-appear"),
				EventType:  stringPtr("should-not-appear"),
				SourceID:   stringPtr("should-not-appear"),
			},
			validate: func(t *testing.T, response web.NodeResponse) {
				t.Helper()
				assert.Equal(t, "action-1", response.ID)
				assert.Equal(t, "log", response.Type)
				assert.Equal(t, "action", response.Category)
				assert.Equal(t, "Log Action", response.Name)
				assert.Equal(t, map[string]any{"message": "test"}, response.Config)
				assert.Equal(t, true, response.Enabled)
				assert.Equal(t, 100, response.PositionX)
				assert.Equal(t, 200, response.PositionY)
				assert.Nil(t, response.ProviderID)
				assert.Nil(t, response.EventType)
			},
		},
		{
			name: "trigger node includes trigger fields",
			node: &models.WorkflowNode{
				ID:         "trigger-1",
				Type:       "trigger:scheduler",
				Category:   models.CategoryTypeTrigger,
				Name:       "Schedule Trigger",
				Config:     map[string]any{"cron": "0 0 * * *"},
				Enabled:    true,
				PositionX:  50,
				PositionY:  50,
				ProviderID: stringPtr("scheduler"),
				EventType:  stringPtr("schedule_due"),
				SourceID:   stringPtr("should-not-appear"),
			},
			validate: func(t *testing.T, response web.NodeResponse) {
				t.Helper()
				assert.Equal(t, "trigger-1", response.ID)
				assert.Equal(t, "trigger:scheduler", response.Type)
				assert.Equal(t, "trigger", response.Category)
				assert.Equal(t, "Schedule Trigger", response.Name)
				assert.Equal(t, map[string]any{"cron": "0 0 * * *"}, response.Config)
				assert.Equal(t, true, response.Enabled)
				assert.Equal(t, 50, response.PositionX)
				assert.Equal(t, 50, response.PositionY)
				assert.NotNil(t, response.ProviderID)
				assert.Equal(t, "scheduler", *response.ProviderID)
				assert.NotNil(t, response.EventType)
				assert.Equal(t, "schedule_due", *response.EventType)
			},
		},
		{
			name: "conditional node excludes trigger fields",
			node: &models.WorkflowNode{
				ID:         "conditional-1",
				Type:       "conditional",
				Category:   models.CategoryTypeAction,
				Name:       "Check Status",
				Config:     map[string]any{"condition": "$.status == 'active'"},
				Enabled:    true,
				PositionX:  200,
				PositionY:  150,
				ProviderID: stringPtr("should-not-appear"),
				EventType:  stringPtr("should-not-appear"),
			},
			validate: func(t *testing.T, response web.NodeResponse) {
				t.Helper()
				assert.Equal(t, "conditional-1", response.ID)
				assert.Equal(t, "conditional", response.Type)
				assert.Equal(t, "action", response.Category)
				assert.Equal(t, "Check Status", response.Name)
				assert.Equal(t, map[string]any{"condition": "$.status == 'active'"}, response.Config)
				assert.Equal(t, true, response.Enabled)
				assert.Equal(t, 200, response.PositionX)
				assert.Equal(t, 150, response.PositionY)
				assert.Nil(t, response.ProviderID)
				assert.Nil(t, response.EventType)
			},
		},
		{
			name: "trigger node with nil fields",
			node: &models.WorkflowNode{
				ID:         "trigger-2",
				Type:       "trigger:webhook",
				Category:   models.CategoryTypeTrigger,
				Name:       "Webhook Trigger",
				Config:     map[string]any{"path": "/webhook"},
				Enabled:    false,
				PositionX:  75,
				PositionY:  125,
				ProviderID: nil,
				EventType:  nil,
			},
			validate: func(t *testing.T, response web.NodeResponse) {
				t.Helper()
				assert.Equal(t, "trigger-2", response.ID)
				assert.Equal(t, "trigger:webhook", response.Type)
				assert.Equal(t, "trigger", response.Category)
				assert.Equal(t, "Webhook Trigger", response.Name)
				assert.Equal(t, false, response.Enabled)
				assert.Nil(t, response.ProviderID)
				assert.Nil(t, response.EventType)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			response := web.TransformNodeResponse(tt.node)
			tt.validate(t, response)
		})
	}
}
