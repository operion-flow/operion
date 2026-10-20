package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/google/uuid"
)

var (
	kafkaNamespace = uuid.MustParse("6f1c3d2e-8a4b-4c5d-9e6f-7a8b9c0d1e2f")

	errUnsupportedFormat = errors.New("event format is not supported yet")
	errNotAJSONObject    = errors.New("payload is not a JSON object")
	errTenantMissing     = errors.New("tenant not found in the event")
)

type KafkaTriggers interface {
	KafkaTriggers(ctx context.Context, topic string) ([]models.TriggerNodeMatch, error)
	EventTriggers(ctx context.Context, eventKey, tenantID string) ([]models.TriggerNodeMatch, error)
}

type EventCatalog interface {
	ByTopic(ctx context.Context, topic string) ([]*models.EventDefinition, error)
}

type KafkaMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

type Kafka struct {
	triggers KafkaTriggers
	catalog  EventCatalog
	jobs     Starter
	logger   *slog.Logger
}

func NewKafka(triggers KafkaTriggers, catalog EventCatalog, jobs Starter, logger *slog.Logger) *Kafka {
	return &Kafka{triggers: triggers, catalog: catalog, jobs: jobs, logger: logger}
}

func (k *Kafka) Handle(ctx context.Context, message KafkaMessage) error {
	matches, err := k.triggers.KafkaTriggers(ctx, message.Topic)
	if err != nil {
		return err
	}

	if err := k.start(ctx, message, matches, kafkaData(message)); err != nil {
		return err
	}

	definitions, err := k.catalog.ByTopic(ctx, message.Topic)
	if err != nil {
		return err
	}

	for _, definition := range definitions {
		if err := k.handleEvent(ctx, message, definition); err != nil {
			return err
		}
	}

	return nil
}

func (k *Kafka) handleEvent(ctx context.Context, message KafkaMessage, definition *models.EventDefinition) error {
	tenant, data, err := eventData(definition, message)
	if err != nil {
		k.logger.WarnContext(ctx, "event skipped",
			"event", definition.Key,
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
			"error", err,
		)

		return nil
	}

	matches, err := k.triggers.EventTriggers(ctx, definition.Key, tenant)
	if err != nil {
		return err
	}

	return k.start(ctx, message, matches, data)
}

func (k *Kafka) start(ctx context.Context, message KafkaMessage, matches []models.TriggerNodeMatch, data map[string]any) error {
	for _, match := range matches {
		source := fmt.Sprintf("%s/%s/%s/%d/%d", match.WorkflowID, match.TriggerNode.ID, message.Topic, message.Partition, message.Offset)
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

	data := messageData(message)
	data["message_key"] = string(message.Key)
	data["message_data"] = value

	return data
}

func eventData(definition *models.EventDefinition, message KafkaMessage) (string, map[string]any, error) {
	if definition.Format != models.EventFormatJSON {
		return "", nil, fmt.Errorf("%w: %s", errUnsupportedFormat, definition.Format)
	}

	var payload map[string]any
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		return "", nil, fmt.Errorf("%w: %w", errNotAJSONObject, err)
	}

	tenant := tenantOf(definition, message, payload)
	if tenant == "" {
		return "", nil, fmt.Errorf("%w: %s %q", errTenantMissing, definition.TenantFrom, definition.TenantKey)
	}

	data := messageData(message)
	data["event"] = definition.Key
	data["tenant_id"] = tenant
	data["payload"] = payload
	data["key"] = string(message.Key)

	return tenant, data, nil
}

func tenantOf(definition *models.EventDefinition, message KafkaMessage, payload map[string]any) string {
	if definition.TenantFrom == models.TenantFromHeader {
		return message.Headers[definition.TenantKey]
	}

	var value any = payload

	for field := range strings.SplitSeq(definition.TenantKey, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			return ""
		}

		value = object[field]
	}

	switch tenant := value.(type) {
	case string:
		return tenant
	case float64:
		return strconv.FormatFloat(tenant, 'f', -1, 64)
	default:
		return ""
	}
}

func messageData(message KafkaMessage) map[string]any {
	headers := make(map[string]any, len(message.Headers))
	for name, header := range message.Headers {
		headers[name] = header
	}

	return map[string]any{
		"topic":     message.Topic,
		"partition": message.Partition,
		"offset":    message.Offset,
		"headers":   headers,
		"timestamp": message.Timestamp.UTC().Format(time.RFC3339),
	}
}
