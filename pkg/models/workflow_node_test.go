package models

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Updated Workflow Model Tests (Node-based)

func TestWorkflow_NodeBased_Validation_ValidWorkflow(t *testing.T) {
	workflow := &Workflow{
		ID:          "wf-123",
		Name:        "User Registration Flow",
		Description: "Handles user registration with email verification",
		Status:      WorkflowStatusDraft,
		Variables:   map[string]any{"api_timeout": 30, "retry_count": 3},
		Owner:       "user-456",
		Nodes: []*WorkflowNode{
			{
				ID:       "validate-input",
				Type:     "conditional",
				Category: CategoryTypeAction,
				Name:     "Validate Input",
				Config:   map[string]any{"expression": "{{.trigger_data.email}} != ''"},
				Enabled:  true,
			},
			{
				ID:      "create-user",
				Type:    "http_request",
				Name:    "Create User",
				Config:  map[string]any{"url": "https://api.example.com/users", "method": "POST"},
				Enabled: true,
			},
		},
		Connections: []*Connection{
			{
				ID:         "conn-1",
				SourcePort: "validate-input:true",
				TargetPort: "create-user:default",
			},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	validate := validator.New()
	err := validate.Struct(workflow)
	assert.NoError(t, err)
}

func TestWorkflow_NodeBased_Validation_MissingRequiredFields(t *testing.T) {
	testCases := []struct {
		name      string
		workflow  *Workflow
		fieldName string
	}{
		{
			name: "missing name",
			workflow: &Workflow{
				ID:          "wf-123",
				Name:        "",
				Description: "Some description",
				Status:      WorkflowStatusDraft,
			},
			fieldName: "Name",
		},
		{
			name: "missing description",
			workflow: &Workflow{
				ID:          "wf-123",
				Name:        "Test Workflow",
				Description: "",
				Status:      WorkflowStatusDraft,
			},
			fieldName: "Description",
		},
		{
			name: "missing status",
			workflow: &Workflow{
				ID:          "wf-123",
				Name:        "Test Workflow",
				Description: "Some description",
				Status:      "",
			},
			fieldName: "Status",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validate := validator.New()
			err := validate.Struct(tc.workflow)
			assert.Error(t, err)

			validationErrors := func() validator.ValidationErrors {
				var target validator.ValidationErrors

				_ = errors.As(err, &target)

				return target
			}()

			found := false

			for _, fieldErr := range validationErrors {
				if fieldErr.Field() == tc.fieldName && (fieldErr.Tag() == requiredTag || fieldErr.Tag() == minTag) {
					found = true

					break
				}
			}

			assert.True(t, found, "Should have validation error for required %s field", tc.fieldName)
		})
	}
}

func TestWorkflow_StatusConstants(t *testing.T) {
	testCases := []struct {
		name   string
		status WorkflowStatus
	}{
		{"draft", WorkflowStatusDraft},
		{"published", WorkflowStatusPublished},
		{"unpublished", WorkflowStatusUnpublished},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			workflow := &Workflow{
				ID:          "wf-123",
				Name:        "Test Workflow",
				Description: "Test workflow description",
				Status:      tc.status,
				CreatedAt:   time.Now().UTC(),
				UpdatedAt:   time.Now().UTC(),
			}

			validate := validator.New()
			err := validate.Struct(workflow)
			assert.NoError(t, err)

			// Test JSON serialization
			jsonData, err := json.Marshal(workflow)
			require.NoError(t, err)
			assert.Contains(t, string(jsonData), `"status":"`+string(tc.status)+`"`)

			var deserialized Workflow

			err = json.Unmarshal(jsonData, &deserialized)
			require.NoError(t, err)
			assert.Equal(t, tc.status, deserialized.Status)
		})
	}
}

func TestWorkflow_SimplifiedVersioning(t *testing.T) {
	const workflowGroupID = "3f2a1b0c-9d8e-4f7a-8b6c-5d4e3f2a1b0c"

	// Test draft workflow
	draft := &Workflow{
		ID:              "wf-draft-123",
		Name:            "My Workflow",
		Description:     "Draft workflow for testing",
		Status:          WorkflowStatusDraft,
		WorkflowGroupID: workflowGroupID,
		Nodes: []*WorkflowNode{
			{
				ID:      "node-1",
				Type:    "http_request",
				Name:    "API Call",
				Config:  map[string]any{"url": "https://api.example.com"},
				Enabled: true,
			},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	// Test published workflow
	published := &Workflow{
		ID:              "wf-published-456",
		Name:            "My Workflow",
		Description:     "Published workflow for testing",
		Status:          WorkflowStatusPublished,
		WorkflowGroupID: workflowGroupID, // Same group as draft
		Nodes: []*WorkflowNode{
			{
				ID:      "node-1",
				Type:    "http_request",
				Name:    "API Call",
				Config:  map[string]any{"url": "https://api.example.com"},
				Enabled: true,
			},
		},
		CreatedAt: time.Now().UTC().Add(-1 * time.Hour),
		UpdatedAt: time.Now().UTC().Add(-1 * time.Hour),
		PublishedAt: func() *time.Time {
			t := time.Now().UTC()

			return &t
		}(),
	}

	// Test unpublished workflow
	unpublished := &Workflow{
		ID:              "wf-unpublished-789",
		Name:            "My Workflow",
		Description:     "Unpublished workflow for testing",
		Status:          WorkflowStatusUnpublished,
		WorkflowGroupID: workflowGroupID, // Same group
		Nodes: []*WorkflowNode{
			{
				ID:      "node-1",
				Type:    "http_request",
				Name:    "API Call",
				Config:  map[string]any{"url": "https://api.example.com"},
				Enabled: true,
			},
		},
		CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
		UpdatedAt: time.Now().UTC().Add(-2 * time.Hour),
	}

	testCases := []struct {
		name     string
		workflow *Workflow
	}{
		{"draft workflow", draft},
		{"published workflow", published},
		{"unpublished workflow", unpublished},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validate := validator.New()
			err := validate.Struct(tc.workflow)
			assert.NoError(t, err)

			// Test JSON serialization
			jsonData, err := json.Marshal(tc.workflow)
			require.NoError(t, err)

			var deserialized Workflow

			err = json.Unmarshal(jsonData, &deserialized)
			require.NoError(t, err)

			assert.Equal(t, tc.workflow.ID, deserialized.ID)
			assert.Equal(t, tc.workflow.Status, deserialized.Status)
			assert.Equal(t, tc.workflow.WorkflowGroupID, deserialized.WorkflowGroupID)

			if tc.workflow.PublishedAt != nil {
				require.NotNil(t, deserialized.PublishedAt)
				assert.WithinDuration(t, *tc.workflow.PublishedAt, *deserialized.PublishedAt, time.Second)
			} else {
				assert.Nil(t, deserialized.PublishedAt)
			}
		})
	}
}

func TestWorkflow_ComplexNodeGraph_JSONSerialization(t *testing.T) {
	original := &Workflow{
		ID:          "wf-complex-123",
		Name:        "Complex Workflow",
		Description: "A workflow demonstrating conditional branching and merging",
		Status:      WorkflowStatusPublished,
		Variables: map[string]any{
			"api_base_url": "https://api.example.com",
			"timeout":      30,
			"retry_count":  3,
		},
		Metadata: map[string]any{
			"category": "data-processing",
			"tags":     []string{"api", "transformation", "conditional"},
		},
		Owner: "user-123",
		Nodes: []*WorkflowNode{
			{
				ID:        "fetch-data",
				Type:      "http_request",
				Name:      "Fetch Data",
				Config:    map[string]any{"url": "{{.variables.api_base_url}}/data", "method": "GET"},
				PositionX: 100,
				PositionY: 100,
				Enabled:   true,
			},
			{
				ID:        "validate-data",
				Type:      "conditional",
				Name:      "Validate Data",
				Config:    map[string]any{"expression": "{{.step_results.fetch_data.status_code}} == 200"},
				PositionX: 300,
				PositionY: 100,
				Enabled:   true,
			},
			{
				ID:        "process-data",
				Type:      "transform",
				Name:      "Process Data",
				Config:    map[string]any{"expression": `{"processed": true, "data": {{.step_results.fetch_data.body}}}`},
				PositionX: 500,
				PositionY: 50,
				Enabled:   true,
			},
			{
				ID:        "handle-error",
				Type:      "log",
				Name:      "Handle Error",
				Config:    map[string]any{"message": "Failed to fetch data: {{.step_results.fetch_data.error}}", "level": "error"},
				PositionX: 500,
				PositionY: 150,
				Enabled:   true,
			},
		},
		Connections: []*Connection{
			{
				ID:         "conn-1",
				SourcePort: "fetch-data:success",
				TargetPort: "validate-data:default",
			},
			{
				ID:         "conn-2",
				SourcePort: "validate-data:true",
				TargetPort: "process-data:default",
			},
			{
				ID:         "conn-3",
				SourcePort: "validate-data:false",
				TargetPort: "handle-error:default",
			},
			{
				ID:         "conn-4",
				SourcePort: "fetch-data:error",
				TargetPort: "handle-error:default",
			},
		},
		CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
		UpdatedAt: time.Now().UTC().Add(-1 * time.Hour),
		PublishedAt: func() *time.Time {
			t := time.Now().UTC()

			return &t
		}(),
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Contains(t, string(jsonData), `"id":"wf-complex-123"`)
	assert.Contains(t, string(jsonData), `"name":"Complex Workflow"`)
	assert.Contains(t, string(jsonData), `"status":"published"`)

	// Deserialize from JSON
	var deserialized Workflow

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify basic fields
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.Description, deserialized.Description)
	assert.Equal(t, original.Status, deserialized.Status)
	assert.Equal(t, original.Owner, deserialized.Owner)

	// Verify variables
	assert.Equal(t, original.Variables["api_base_url"], deserialized.Variables["api_base_url"])
	assert.Equal(t, float64(30), deserialized.Variables["timeout"]) // JSON numbers become float64
	assert.Equal(t, float64(3), deserialized.Variables["retry_count"])

	// Verify metadata
	assert.Equal(t, original.Metadata["category"], deserialized.Metadata["category"])
	tags := deserialized.Metadata["tags"].([]any)
	assert.Len(t, tags, 3)
	assert.Contains(t, tags, "api")
	assert.Contains(t, tags, "transformation")
	assert.Contains(t, tags, "conditional")

	// Verify nodes
	require.Len(t, deserialized.Nodes, 4)

	nodeMap := make(map[string]*WorkflowNode)
	for _, node := range deserialized.Nodes {
		nodeMap[node.ID] = node
	}

	fetchNode := nodeMap["fetch-data"]
	require.NotNil(t, fetchNode)
	assert.Equal(t, "http_request", fetchNode.Type)
	assert.Equal(t, "Fetch Data", fetchNode.Name)
	assert.Equal(t, 100, fetchNode.PositionX)
	assert.Equal(t, 100, fetchNode.PositionY)
	assert.True(t, fetchNode.Enabled)

	// Verify connections
	require.Len(t, deserialized.Connections, 4)

	connMap := make(map[string]*Connection)
	for _, conn := range deserialized.Connections {
		connMap[conn.ID] = conn
	}

	conn1 := connMap["conn-1"]
	require.NotNil(t, conn1)
	assert.Equal(t, "fetch-data:success", conn1.SourcePort)
	assert.Equal(t, "validate-data:default", conn1.TargetPort)

	// Verify timestamps
	assert.WithinDuration(t, original.CreatedAt, deserialized.CreatedAt, time.Second)
	assert.WithinDuration(t, original.UpdatedAt, deserialized.UpdatedAt, time.Second)
	require.NotNil(t, deserialized.PublishedAt)
	assert.WithinDuration(t, *original.PublishedAt, *deserialized.PublishedAt, time.Second)
}

// ExecutionContext Model Tests (Node-based)

func TestExecutionContext_NodeBased_Validation_ValidContext(t *testing.T) {
	context := &ExecutionContext{
		ID:         "exec-123",
		WorkflowID: "wf-published-456",
		Status:     ExecutionStatusRunning,
		NodeResults: map[string]NodeResult{
			"node-1": {
				NodeID:    "node-1",
				Status:    "success",
				Data:      map[string]any{"response": "ok"},
				Timestamp: time.Now().UTC(),
			},
		},
		TriggerData: map[string]any{"webhook_url": "https://api.example.com/webhook"},
		Variables:   map[string]any{"api_key": "secret-key-123"},
		CreatedAt:   time.Now().UTC(),
	}

	validate := validator.New()
	err := validate.Struct(context)
	assert.NoError(t, err)
}

func TestExecutionContext_Validation_MissingWorkflowID(t *testing.T) {
	context := &ExecutionContext{
		ID:          "exec-123",
		WorkflowID:  "", // Missing required field
		Status:      ExecutionStatusRunning,
		NodeResults: map[string]NodeResult{},
		CreatedAt:   time.Now().UTC(),
	}

	validate := validator.New()
	err := validate.Struct(context)
	assert.Error(t, err)

	validationErrors := func() validator.ValidationErrors {
		var target validator.ValidationErrors

		_ = errors.As(err, &target)

		return target
	}()

	found := false

	for _, fieldErr := range validationErrors {
		if fieldErr.Field() == "WorkflowID" && fieldErr.Tag() == requiredTag {
			found = true

			break
		}
	}

	assert.True(t, found, "Should have validation error for required WorkflowID field")
}

func TestExecutionContext_StatusConstants(t *testing.T) {
	testCases := []struct {
		name   string
		status ExecutionStatus
	}{
		{"running", ExecutionStatusRunning},
		{"completed", ExecutionStatusCompleted},
		{"failed", ExecutionStatusFailed},
		{"cancelled", ExecutionStatusCancelled},
		{"timeout", ExecutionStatusTimeout},
		{"paused", ExecutionStatusPaused},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			context := &ExecutionContext{
				ID:          "exec-123",
				WorkflowID:  "wf-published-456",
				Status:      tc.status,
				NodeResults: map[string]NodeResult{},
				CreatedAt:   time.Now().UTC(),
			}

			validate := validator.New()
			err := validate.Struct(context)
			assert.NoError(t, err)

			// Test JSON serialization
			jsonData, err := json.Marshal(context)
			require.NoError(t, err)
			assert.Contains(t, string(jsonData), `"status":"`+string(tc.status)+`"`)

			var deserialized ExecutionContext

			err = json.Unmarshal(jsonData, &deserialized)
			require.NoError(t, err)
			assert.Equal(t, tc.status, deserialized.Status)
		})
	}
}

func TestExecutionContext_CompleteLifecycle_JSONSerialization(t *testing.T) {
	startTime := time.Now().UTC().Add(-5 * time.Minute)
	endTime := time.Now().UTC()

	original := &ExecutionContext{
		ID:         "exec-complex-789",
		WorkflowID: "wf-published-456",
		Status:     ExecutionStatusCompleted,
		NodeResults: map[string]NodeResult{
			"fetch-data": {
				NodeID:    "fetch-data",
				Status:    "success",
				Data:      map[string]any{"status_code": 200, "body": map[string]any{"users": []map[string]any{{"id": 1, "name": "John"}}}},
				Timestamp: startTime.Add(1 * time.Minute),
			},
			"validate-data": {
				NodeID:    "validate-data",
				Status:    "success",
				Data:      map[string]any{"condition": true, "validated": true},
				Timestamp: startTime.Add(2 * time.Minute),
			},
			"process-data": {
				NodeID:    "process-data",
				Status:    "success",
				Data:      map[string]any{"processed": true, "count": 1, "result": map[string]any{"transformed": true}},
				Timestamp: startTime.Add(3 * time.Minute),
			},
		},
		TriggerData: map[string]any{
			"webhook": map[string]any{
				"url":    "https://api.example.com/webhook",
				"method": "POST",
				"headers": map[string]string{
					"Content-Type":  "application/json",
					"Authorization": "Bearer token-123",
				},
			},
		},
		Variables: map[string]any{
			"api_base_url": "https://api.example.com",
			"timeout":      30,
			"retry_count":  3,
			"debug_mode":   true,
			"user_filters": []string{"active", "verified"},
		},
		Metadata: map[string]any{
			"source":      "webhook",
			"environment": "production",
			"region":      "us-east-1",
		},
		ErrorMessage: "",
		CreatedAt:    startTime,
		CompletedAt:  &endTime,
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Contains(t, string(jsonData), `"id":"exec-complex-789"`)
	assert.Contains(t, string(jsonData), `"workflow_id":"wf-published-456"`)
	assert.Contains(t, string(jsonData), `"status":"completed"`)

	// Deserialize from JSON
	var deserialized ExecutionContext

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify basic fields
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.WorkflowID, deserialized.WorkflowID)
	assert.Equal(t, original.Status, deserialized.Status)
	assert.Equal(t, original.ErrorMessage, deserialized.ErrorMessage)

	// Verify timestamps
	assert.WithinDuration(t, original.CreatedAt, deserialized.CreatedAt, time.Second)
	require.NotNil(t, deserialized.CompletedAt)
	assert.WithinDuration(t, *original.CompletedAt, *deserialized.CompletedAt, time.Second)

	// Verify node results
	assert.Len(t, deserialized.NodeResults, 3)

	fetchResult := deserialized.NodeResults["fetch-data"]
	assert.Equal(t, "fetch-data", fetchResult.NodeID)
	assert.Equal(t, "success", fetchResult.Status)
	assert.Equal(t, float64(200), fetchResult.Data["status_code"])

	validateResult := deserialized.NodeResults["validate-data"]
	assert.Equal(t, true, validateResult.Data["condition"])
	assert.Equal(t, true, validateResult.Data["validated"])

	// Verify trigger data
	webhookData := deserialized.TriggerData["webhook"].(map[string]any)
	assert.Equal(t, "https://api.example.com/webhook", webhookData["url"])
	assert.Equal(t, "POST", webhookData["method"])

	// Verify variables
	assert.Equal(t, original.Variables["api_base_url"], deserialized.Variables["api_base_url"])
	assert.Equal(t, float64(30), deserialized.Variables["timeout"])
	assert.Equal(t, true, deserialized.Variables["debug_mode"])

	filters := deserialized.Variables["user_filters"].([]any)
	assert.Len(t, filters, 2)
	assert.Contains(t, filters, "active")
	assert.Contains(t, filters, "verified")

	// Verify metadata
	assert.Equal(t, original.Metadata["source"], deserialized.Metadata["source"])
	assert.Equal(t, original.Metadata["environment"], deserialized.Metadata["environment"])
	assert.Equal(t, original.Metadata["region"], deserialized.Metadata["region"])
}
