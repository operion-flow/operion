package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/google/uuid"
)

var kafkaNamespace = uuid.MustParse("6f1c3d2e-8a4b-4c5d-9e6f-7a8b9c0d1e2f")

type KafkaTriggers interface {
	KafkaTriggers(ctx context.Context, topic string) ([]models.TriggerNodeMatch, error)
}

type KafkaMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

type Kafka struct {
	triggers KafkaTriggers
	jobs     Starter
	logger   *slog.Logger
}

func NewKafka(triggers KafkaTriggers, jobs Starter, logger *slog.Logger) *Kafka {
	return &Kafka{triggers: triggers, jobs: jobs, logger: logger}
}

func (k *Kafka) Handle(ctx context.Context, message KafkaMessage) error {
	matches, err := k.triggers.KafkaTriggers(ctx, message.Topic)
	if err != nil {
		return err
	}

	data := kafkaData(message)

	for _, match := range matches {
		source := fmt.Sprintf("%s/%s/%d/%d", match.WorkflowID, message.Topic, message.Partition, message.Offset)
		executionID := uuid.NewSHA1(kafkaNamespace, []byte(source)).String()
		activation := engine.TriggerActivation(match.TriggerNode.ID, data)
		now := time.Now().UTC()
		execution := &models.ExecutionContext{
			ID:          executionID,
			WorkflowID:  match.WorkflowID,
			Status:      models.ExecutionStatusRunning,
			NodeResults: map[string]models.NodeResult{},
			TriggerData: data,
			CreatedAt:   now,
		}
		job := models.Job{
			ID:          uuid.NewSHA1(kafkaNamespace, []byte(source+"/job")).String(),
			ExecutionID: executionID,
			NodeID:      activation.NodeID,
			Port:        activation.Port,
			Data:        activation.Data,
			RunAt:       now,
		}

		if err := k.jobs.Start(ctx, execution, job); err != nil {
			return err
		}

		k.logger.InfoContext(ctx, "execution started",
			"execution_id", executionID,
			"workflow_id", match.WorkflowID,
			"node_id", activation.NodeID,
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
		)
	}

	return nil
}

func kafkaData(message KafkaMessage) map[string]any {
	var value any
	if err := json.Unmarshal(message.Value, &value); err != nil {
		value = map[string]any{"raw_message": string(message.Value)}
	}

	headers := make(map[string]any, len(message.Headers))
	for name, header := range message.Headers {
		headers[name] = header
	}

	return map[string]any{
		"topic":        message.Topic,
		"partition":    message.Partition,
		"offset":       message.Offset,
		"message_key":  string(message.Key),
		"message_data": value,
		"headers":      headers,
		"timestamp":    message.Timestamp.UTC().Format(time.RFC3339),
	}
}
