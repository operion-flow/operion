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

const (
	requiredTag = "required"
	minTag      = "min"
)

// Connection Model Tests

func TestConnection_Validation_ValidConnection(t *testing.T) {
	connection := &Connection{
		ID:         "conn-123",
		SourcePort: "node-1:success",
		TargetPort: "node-2:input",
	}

	validate := validator.New()
	err := validate.Struct(connection)
	assert.NoError(t, err)
}

func TestConnection_Validation_MissingFields(t *testing.T) {
	testCases := []struct {
		name       string
		connection *Connection
		fieldName  string
	}{
		{
			name: "missing source port",
			connection: &Connection{
				ID:         "conn-123",
				SourcePort: "",
				TargetPort: "node-2:input",
			},
			fieldName: "SourcePort",
		},
		{
			name: "missing target port",
			connection: &Connection{
				ID:         "conn-123",
				SourcePort: "node-1:success",
				TargetPort: "",
			},
			fieldName: "TargetPort",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validate := validator.New()
			err := validate.Struct(tc.connection)
			assert.Error(t, err)

			validationErrors := func() validator.ValidationErrors {
				var target validator.ValidationErrors

				_ = errors.As(err, &target)

				return target
			}()

			found := false

			for _, fieldErr := range validationErrors {
				if fieldErr.Field() == tc.fieldName && fieldErr.Tag() == requiredTag {
					found = true

					break
				}
			}

			assert.True(t, found, "Should have validation error for required %s field", tc.fieldName)
		})
	}
}

func TestConnection_JSONSerialization(t *testing.T) {
	original := &Connection{
		ID:         "conn-123",
		SourcePort: "conditional-node-1:true",
		TargetPort: "http-request-node-2:default",
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Contains(t, string(jsonData), `"id":"conn-123"`)
	assert.Contains(t, string(jsonData), `"source_port":"conditional-node-1:true"`)
	assert.Contains(t, string(jsonData), `"target_port":"http-request-node-2:default"`)

	// Deserialize from JSON
	var deserialized Connection

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.SourcePort, deserialized.SourcePort)
	assert.Equal(t, original.TargetPort, deserialized.TargetPort)
}

// WorkflowNode Model Tests

func TestWorkflowNode_Validation_ValidNode(t *testing.T) {
	node := &WorkflowNode{
		ID:        "node-123",
		Type:      "httprequest",
		Category:  CategoryTypeAction,
		Name:      "API Call",
		Config:    map[string]any{"url": "https://api.example.com", "method": "GET"},
		PositionX: 100,
		PositionY: 200,
	}

	validate := validator.New()
	err := validate.Struct(node)
	assert.NoError(t, err)
}

func TestWorkflowNode_Validation_MissingFields(t *testing.T) {
	testCases := []struct {
		name      string
		node      *WorkflowNode
		fieldName string
	}{
		{
			name: "missing id",
			node: &WorkflowNode{
				ID:       "",
				Type:     "httprequest",
				Category: CategoryTypeAction,
				Name:     "API Call",
			},
			fieldName: "ID",
		},
		{
			name: "missing type",
			node: &WorkflowNode{
				ID:       "node-123",
				Type:     "",
				Category: CategoryTypeAction,
				Name:     "API Call",
			},
			fieldName: "Type",
		},
		{
			name: "missing name",
			node: &WorkflowNode{
				ID:       "node-123",
				Type:     "httprequest",
				Category: CategoryTypeAction,
				Name:     "",
			},
			fieldName: "Name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validate := validator.New()
			err := validate.Struct(tc.node)
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

func TestWorkflowNode_JSONSerialization(t *testing.T) {
	original := &WorkflowNode{
		ID:       "node-123",
		Type:     "httprequest",
		Category: CategoryTypeAction,
		Name:     "Fetch User Data",
		Config: map[string]any{
			"url":     "https://api.example.com/users/{{.trigger_data.user_id}}",
			"method":  "GET",
			"headers": map[string]string{"Authorization": "Bearer {{.variables.api_token}}"},
		},
		Retry:     &RetryPolicy{Attempts: 3, Initial: "1s", Max: "1m"},
		PositionX: 250,
		PositionY: 150,
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Contains(t, string(jsonData), `"id":"node-123"`)
	assert.Contains(t, string(jsonData), `"type":"httprequest"`)
	assert.Contains(t, string(jsonData), `"name":"Fetch User Data"`)
	assert.Contains(t, string(jsonData), `"retry":{"attempts":3,"initial":"1s","max":"1m"}`)

	// Deserialize from JSON
	var deserialized WorkflowNode

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.Type, deserialized.Type)
	assert.Equal(t, original.Category, deserialized.Category)
	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.PositionX, deserialized.PositionX)
	assert.Equal(t, original.PositionY, deserialized.PositionY)
	assert.Equal(t, original.Retry, deserialized.Retry)

	// Verify config deep equality
	assert.Equal(t, original.Config["url"], deserialized.Config["url"])
	assert.Equal(t, original.Config["method"], deserialized.Config["method"])
}

// NodeResult Model Tests

func TestNodeResult_JSONSerialization(t *testing.T) {
	original := &NodeResult{
		NodeID:    "node-123",
		Status:    "success",
		Data:      map[string]any{"response_code": 200, "body": map[string]any{"id": 42, "name": "John Doe"}},
		Timestamp: time.Now().UTC(),
		Error:     "",
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Contains(t, string(jsonData), `"node_id":"node-123"`)
	assert.Contains(t, string(jsonData), `"status":"success"`)

	// Deserialize from JSON
	var deserialized NodeResult

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.NodeID, deserialized.NodeID)
	assert.Equal(t, original.Status, deserialized.Status)
	assert.Equal(t, original.Error, deserialized.Error)
	assert.WithinDuration(t, original.Timestamp, deserialized.Timestamp, time.Second)

	// Verify data deep equality
	assert.Equal(t, float64(200), deserialized.Data["response_code"]) // JSON numbers become float64
	bodyData := deserialized.Data["body"].(map[string]any)
	assert.Equal(t, float64(42), bodyData["id"])
	assert.Equal(t, "John Doe", bodyData["name"])
}

func TestNodeResult_WithError(t *testing.T) {
	original := &NodeResult{
		NodeID:    "node-456",
		Status:    "error",
		Data:      map[string]any{},
		Timestamp: time.Now().UTC(),
		Error:     "HTTP request failed: 404 Not Found",
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Contains(t, string(jsonData), `"status":"error"`)
	assert.Contains(t, string(jsonData), `"error":"HTTP request failed: 404 Not Found"`)

	// Deserialize from JSON
	var deserialized NodeResult

	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	assert.Equal(t, original.NodeID, deserialized.NodeID)
	assert.Equal(t, original.Status, deserialized.Status)
	assert.Equal(t, original.Error, deserialized.Error)
	assert.WithinDuration(t, original.Timestamp, deserialized.Timestamp, time.Second)
}
