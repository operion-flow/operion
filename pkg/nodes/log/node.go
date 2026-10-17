// Package log provides logging node implementation for workflow graph execution.
package log

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
)

const (
	OutputPortSuccess = "success"
	OutputPortError   = "error"
	InputPortMain     = "main"
)

// LogLevel represents different logging levels.
type LogLevel int

const (
	Debug LogLevel = iota
	Info
	Warn
	Error
)

var logLevelName = map[LogLevel]string{
	Debug: "debug",
	Info:  "info",
	Warn:  "warn",
	Error: "error",
}

// LogNode implements the Node interface for logging messages.
type LogNode struct {
	id      string
	message string
	level   string
	logger  *slog.Logger
}

// NewLogNode creates a new logging node.
func NewLogNode(id string, config map[string]any) (*LogNode, error) {
	// Parse message (required)
	message, ok := config["message"].(string)
	if !ok {
		return nil, errors.New("missing required field 'message'")
	}

	// Parse level (optional, defaults to "info")
	level := logLevelName[Info]
	if lvl, ok := config["level"].(string); ok {
		level = lvl
	}

	return &LogNode{
		id:      id,
		message: message,
		level:   level,
		logger:  slog.Default(), // Will be replaced with proper logger during execution
	}, nil
}

// ID returns the node ID.
func (n *LogNode) ID() string {
	return n.id
}

// Type returns the node type.
func (n *LogNode) Type() string {
	return "log"
}

// Execute performs the logging operation.
func (n *LogNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	// Render the message with templating if needed
	renderedMessage, err := template.RenderWithContext(n.message, &ctx)
	if err != nil {
		return n.createErrorResult(fmt.Sprintf("failed to render log message template: %v", err)), nil
	}

	message := fmt.Sprintf("%v", renderedMessage)

	// TODO: In a full implementation, this would use the logger from the execution context
	// For now, we'll use the default logger
	logger := n.logger.With("node_id", n.id, "node_type", "log")

	// Log the message at the specified level
	switch n.level {
	case logLevelName[Debug]:
		logger.Debug(message)
	case logLevelName[Info]:
		logger.Info(message)
	case logLevelName[Warn]:
		logger.Warn(message)
	case logLevelName[Error]:
		logger.Error(message)
	default:
		logger.Info(message)
	}

	// Return success result
	return map[string]models.NodeResult{
		OutputPortSuccess: {
			NodeID: n.id,
			Data: map[string]any{
				"message": message,
				"level":   n.level,
				"logged":  true,
			},
			Status: string(models.NodeStatusSuccess),
		},
	}, nil
}

// createErrorResult creates a NodeResult for the error output port.
func (n *LogNode) createErrorResult(errorMessage string) map[string]models.NodeResult {
	return map[string]models.NodeResult{
		OutputPortError: {
			NodeID: n.id,
			Data: map[string]any{
				"error":   errorMessage,
				"success": false,
			},
			Status: string(models.NodeStatusError),
		},
	}
}

// InputPorts returns the input ports for the node.
func (n *LogNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, InputPortMain),
				NodeID:      n.id,
				Name:        InputPortMain,
				Description: "Main input for triggering the log operation",
			},
		},
	}
}

// OutputPorts returns the output ports for the node.
func (n *LogNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortSuccess),
				NodeID:      n.id,
				Name:        OutputPortSuccess,
				Description: "Logged message information",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"message": map[string]any{"type": "string", "description": "The logged message"},
						"level":   map[string]any{"type": "string", "description": "The log level used"},
						"logged":  map[string]any{"type": "boolean", "description": "Whether the message was successfully logged"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, OutputPortError),
				NodeID:      n.id,
				Name:        OutputPortError,
				Description: "Error information when logging fails",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"error":   map[string]any{"type": "string"},
						"success": map[string]any{"type": "boolean"},
					},
				},
			},
		},
	}
}

// Validate validates the node configuration.
func (n *LogNode) Validate(config map[string]any) error {
	// Validate message is present
	if _, ok := config["message"]; !ok {
		return errors.New("missing required field 'message'")
	}

	// Validate level if provided
	if level, ok := config["level"].(string); ok {
		validLevels := map[string]bool{
			logLevelName[Debug]: true,
			logLevelName[Info]:  true,
			logLevelName[Warn]:  true,
			logLevelName[Error]: true,
		}
		if !validLevels[level] {
			return fmt.Errorf("invalid log level '%s' (must be debug, info, warn, or error)", level)
		}
	}

	return nil
}
