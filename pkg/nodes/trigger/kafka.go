package trigger

import (
	"errors"

	"github.com/dukex/operion/pkg/models"
)

const (
	KafkaInputPortExternal = "external"
	KafkaOutputPortSuccess = "success"
	KafkaOutputPortError   = "error"
)

// KafkaTriggerNode implements the Node interface for Kafka triggers.
type KafkaTriggerNode struct {
	id     string
	config KafkaTriggerConfig
}

// KafkaTriggerConfig defines the configuration for Kafka trigger nodes.
type KafkaTriggerConfig struct {
	Topic         string   `json:"topic"`
	ConsumerGroup string   `json:"consumer_group"`
	Brokers       []string `json:"brokers"`
}

// NewKafkaTriggerNode creates a new Kafka trigger node.
func NewKafkaTriggerNode(id string, config map[string]any) (*KafkaTriggerNode, error) {
	// Parse configuration
	kafkaConfig := KafkaTriggerConfig{
		Brokers: []string{},
	}

	// Parse topic (required)
	if topic, ok := config["topic"].(string); ok {
		kafkaConfig.Topic = topic
	} else {
		return nil, errors.New("topic is required")
	}

	// Parse consumer_group (required)
	if consumerGroup, ok := config["consumer_group"].(string); ok {
		kafkaConfig.ConsumerGroup = consumerGroup
	} else {
		return nil, errors.New("consumer_group is required")
	}

	// Parse brokers (required)
	if brokers, ok := config["brokers"].([]any); ok {
		for _, broker := range brokers {
			if brokerStr, ok := broker.(string); ok {
				kafkaConfig.Brokers = append(kafkaConfig.Brokers, brokerStr)
			}
		}
	}

	if len(kafkaConfig.Brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}

	return &KafkaTriggerNode{
		id:     id,
		config: kafkaConfig,
	}, nil
}

// ID returns the node ID.
func (n *KafkaTriggerNode) ID() string {
	return n.id
}

// Type returns the node type.
func (n *KafkaTriggerNode) Type() string {
	return models.NodeTypeTriggerKafka
}

// Execute processes the Kafka message data from external input.
func (n *KafkaTriggerNode) Execute(ctx models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	results := make(map[string]models.NodeResult)

	// Get external input
	externalInput, exists := inputs[KafkaInputPortExternal]
	if !exists {
		return n.createErrorResult("external input not found"), nil
	}

	// Process Kafka data
	kafkaData := externalInput.Data

	// Create success result with Kafka message data
	results[KafkaOutputPortSuccess] = models.NodeResult{
		NodeID: n.id,
		Data: map[string]any{
			"topic":     kafkaData["topic"],
			"partition": kafkaData["partition"],
			"offset":    kafkaData["offset"],
			"key":       kafkaData["message_key"],
			"message":   kafkaData["message_data"],
			"headers":   kafkaData["headers"],
			"timestamp": kafkaData["timestamp"],
			"config":    n.config,
		},
		Status: string(models.NodeStatusSuccess),
	}

	return results, nil
}

// createErrorResult creates an error result for the error output port.
func (n *KafkaTriggerNode) createErrorResult(message string) map[string]models.NodeResult {
	return map[string]models.NodeResult{
		KafkaOutputPortError: {
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

// InputPorts returns the input ports for the Kafka trigger node.
func (n *KafkaTriggerNode) InputPorts() []models.InputPort {
	return []models.InputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, KafkaInputPortExternal),
				NodeID:      n.id,
				Name:        KafkaInputPortExternal,
				Description: "External Kafka message input",
				Schema: map[string]any{
					"type":        "object",
					"description": "Kafka message data from external source",
					"properties": map[string]any{
						"topic":        map[string]any{"type": "string"},
						"partition":    map[string]any{"type": "integer"},
						"offset":       map[string]any{"type": "integer"},
						"message_key":  map[string]any{"type": "string"},
						"message_data": map[string]any{"type": "string"},
						"headers":      map[string]any{"type": "object"},
						"timestamp":    map[string]any{"type": "string", "format": "date-time"},
					},
				},
			},
		},
	}
}

// OutputPorts returns the output ports for the Kafka trigger node.
func (n *KafkaTriggerNode) OutputPorts() []models.OutputPort {
	return []models.OutputPort{
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, KafkaOutputPortSuccess),
				NodeID:      n.id,
				Name:        KafkaOutputPortSuccess,
				Description: "Successful Kafka message processing result",
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"topic":     map[string]any{"type": "string"},
						"partition": map[string]any{"type": "integer"},
						"offset":    map[string]any{"type": "integer"},
						"key":       map[string]any{"type": "string"},
						"message":   map[string]any{"type": "string"},
						"headers":   map[string]any{"type": "object"},
						"timestamp": map[string]any{"type": "string", "format": "date-time"},
						"config":    map[string]any{"type": "object"},
					},
				},
			},
		},
		{
			Port: models.Port{
				ID:          models.MakePortID(n.id, KafkaOutputPortError),
				NodeID:      n.id,
				Name:        KafkaOutputPortError,
				Description: "Kafka message processing error",
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
func (n *KafkaTriggerNode) Validate(config map[string]any) error {
	if topic, ok := config["topic"].(string); !ok || topic == "" {
		return errors.New("topic is required and must be a non-empty string")
	}

	if consumerGroup, ok := config["consumer_group"].(string); !ok || consumerGroup == "" {
		return errors.New("consumer_group is required and must be a non-empty string")
	}

	if brokers, ok := config["brokers"].([]any); !ok || len(brokers) == 0 {
		return errors.New("brokers is required and must be a non-empty array")
	}

	return nil
}
