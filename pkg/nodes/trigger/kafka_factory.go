package trigger

import (
	"context"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
)

// KafkaTriggerNodeFactory creates KafkaTriggerNode instances.
type KafkaTriggerNodeFactory struct{}

// NewKafkaTriggerNodeFactory creates a new Kafka trigger node factory.
const schemaRequired = "required"

func NewKafkaTriggerNodeFactory() protocol.NodeFactory {
	return &KafkaTriggerNodeFactory{}
}

// Create creates a new KafkaTriggerNode instance.
func (f *KafkaTriggerNodeFactory) Create(ctx context.Context, id string, config map[string]any) (protocol.Node, error) {
	return NewKafkaTriggerNode(id, config)
}

// ID returns the factory ID.
func (f *KafkaTriggerNodeFactory) ID() string {
	return models.NodeTypeTriggerKafka
}

// Name returns the factory name.
func (f *KafkaTriggerNodeFactory) Name() string {
	return "Kafka Trigger"
}

// Description returns the factory description.
func (f *KafkaTriggerNodeFactory) Description() string {
	return "Receives Kafka messages from specified topics and starts workflow execution"
}

// Schema returns the JSON schema for Kafka trigger node configuration.
func (f *KafkaTriggerNodeFactory) Schema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"topic": map[string]any{
				"type":        "string",
				"description": "Kafka topic to consume messages from",
				"examples": []string{
					"user-events",
					"order-notifications",
					"system-alerts",
				},
			},
			"consumer_group": map[string]any{
				"type":        "string",
				"description": "Kafka consumer group for this trigger",
				"examples": []string{
					"operion-workflow-group",
					"order-processing",
					"notification-handlers",
				},
			},
			"brokers": map[string]any{
				"type":        "array",
				"description": "List of Kafka broker addresses",
				"items": map[string]any{
					"type": "string",
				},
				"minItems": 1,
				"examples": [][]string{
					{"localhost:9092"},
					{"broker1:9092", "broker2:9092", "broker3:9092"},
					{"kafka.example.com:9092"},
				},
			},
		},
		schemaRequired: []string{"topic"},
		"examples": []map[string]any{
			{
				"topic":          "user-events",
				"consumer_group": "operion-workflow",
				"brokers":        []string{"localhost:9092"},
			},
			{
				"topic":          "order-notifications",
				"consumer_group": "order-processing",
				"brokers":        []string{"broker1:9092", "broker2:9092"},
			},
		},
	}
}
