package trigger

import (
	"errors"
	"fmt"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/robfig/cron/v3"
)

const (
	SchedulerInputPortExternal = "external"
	SchedulerOutputPortSuccess = "success"
	SchedulerOutputPortError   = "error"
)

// SchedulerTriggerNode implements the Node interface for scheduler triggers.
type SchedulerTriggerNode struct {
	id     string
	config SchedulerTriggerConfig
}

// SchedulerTriggerConfig defines the configuration for scheduler trigger nodes.
type SchedulerTriggerConfig struct {
	CronExpression string `json:"cron_expression"`
	Timezone       string `json:"timezone"`
}

// NewSchedulerTriggerNode creates a new scheduler trigger node.
func NewSchedulerTriggerNode(id string, config map[string]any) (*SchedulerTriggerNode, error) {
	// Parse configuration
	schedulerConfig := SchedulerTriggerConfig{
		Timezone: "UTC",
	}

	// Parse cron_expression (required)
	if cronExpr, ok := config["cron_expression"].(string); ok {
		schedulerConfig.CronExpression = cronExpr
	} else {
		return nil, errors.New("cron_expression is required")
	}

	// Parse timezone
	if timezone, ok := config["timezone"].(string); ok {
		schedulerConfig.Timezone = timezone
	}

	return &SchedulerTriggerNode{
		id:     id,
		config: schedulerConfig,
	}, nil
}

// ID returns the node ID.
func (n *SchedulerTriggerNode) ID() string {
	return n.id
}

// Type returns the node type.
func (n *SchedulerTriggerNode) Type() string {
	return models.NodeTypeTriggerScheduler
}

// Execute processes the scheduler event data from external input.
func (n *SchedulerTriggerNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	results := make(map[string]models.NodeResult)

	// Get external input
	externalInput, exists := inputs[SchedulerInputPortExternal]
	if !exists {
		return n.createErrorResult("external input not found"), nil
	}

	// Process scheduler data
	schedulerData := externalInput.Data

	// Create success result with scheduler data
	results[SchedulerOutputPortSuccess] = models.NodeResult{
		NodeID: n.id,
		Data: map[string]any{
			"scheduled_time":  schedulerData["scheduled_time"],
			"cron_expression": n.config.CronExpression,
			"execution_time":  time.Now(),
			"timezone":        n.config.Timezone,
			"trigger_data":    schedulerData,
		},
		Status: string(models.NodeStatusSuccess),
	}

	return results, nil
}

// createErrorResult creates an error result for the error output port.
func (n *SchedulerTriggerNode) createErrorResult(message string) map[string]models.NodeResult {
	return map[string]models.NodeResult{
		SchedulerOutputPortError: {
			NodeID: n.id,
			Data: map[string]any{
				"error":   message,
				"node_id": n.id,
			},
			Status: string(models.NodeStatusError),
			Error:  message,
		},
	}
}

// InputPorts returns the input ports for the scheduler trigger node.
func (n *SchedulerTriggerNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, SchedulerInputPortExternal),
				NodeID:      n.id,
				Name:        SchedulerInputPortExternal,
				Description: "External scheduler event input",
				Schema: map[string]any{
					"type":        "object",
					"description": "Scheduler event data from external source",
					"properties": map[string]any{
						"scheduled_time":  map[string]any{"type": "string", "format": "date-time"},
						"cron_expression": map[string]any{"type": "string"},
						"timezone":        map[string]any{"type": "string"},
					},
				},
			},
		},
	}
}

// OutputPorts returns the output ports for the scheduler trigger node.
func (n *SchedulerTriggerNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, SchedulerOutputPortSuccess),
				NodeID:      n.id,
				Name:        SchedulerOutputPortSuccess,
				Description: "Successful scheduler processing result",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"scheduled_time":  map[string]any{"type": "string", "format": "date-time"},
						"cron_expression": map[string]any{"type": "string"},
						"execution_time":  map[string]any{"type": "string", "format": "date-time"},
						"timezone":        map[string]any{"type": "string"},
						"trigger_data":    map[string]any{"type": "object"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, SchedulerOutputPortError),
				NodeID:      n.id,
				Name:        SchedulerOutputPortError,
				Description: "Scheduler processing error",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"error":   map[string]any{"type": "string"},
						"node_id": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
}

// Validate validates the node configuration.
func (n *SchedulerTriggerNode) Validate(config map[string]any) error {
	cronExpr, ok := config["cron_expression"].(string)
	if !ok || cronExpr == "" {
		return errors.New("cron_expression is required and must be a non-empty string")
	}

	if _, err := cron.ParseStandard(cronExpr); err != nil {
		return fmt.Errorf("invalid cron_expression: %w", err)
	}

	if timezone, ok := config["timezone"].(string); ok && timezone != "" {
		// Basic timezone validation
		_, err := time.LoadLocation(timezone)
		if err != nil {
			return errors.New("invalid timezone format")
		}
	}

	return nil
}
