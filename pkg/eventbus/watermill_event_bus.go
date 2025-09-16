package eventbus

import (
	"context"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dukex/operion/pkg/events"
)

type WatermillEventBus struct {
	publisher     message.Publisher
	subscriber    message.Subscriber
	subscriptions map[events.EventType]EventHandler
}

func NewWatermillEventBus(pub message.Publisher, sub message.Subscriber) EventBus {
	return &WatermillEventBus{
		publisher:     pub,
		subscriber:    sub,
		subscriptions: make(map[events.EventType]EventHandler),
	}
}

func (eb *WatermillEventBus) GenerateID() string {
	return watermill.NewULID()
}

func (eb *WatermillEventBus) Publish(ctx context.Context, key string, event Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := message.NewMessage("msg-"+eb.GenerateID(), payload)
	msg.Metadata.Set(events.EventMetadataKey, key)
	msg.Metadata.Set(events.EventTypeMetadataKey, string(event.GetType()))

	return eb.publisher.Publish(events.Topic, msg)
}

func (eb *WatermillEventBus) Subscribe(ctx context.Context) error {
	messages, err := eb.subscriber.Subscribe(ctx, events.Topic)
	if err != nil {
		return err
	}

	go func() {
		for msg := range messages {
			var event any

			eventType := events.EventType(msg.Metadata.Get(events.EventTypeMetadataKey))

			handler, exists := eb.subscriptions[eventType]
			if !exists {
				msg.Ack()

				continue
			}

			switch eventType {
			case events.WorkflowTriggeredEvent:
				event = &events.WorkflowTriggered{}
			case events.WorkflowFinishedEvent:
				event = &events.WorkflowFinished{}
			case events.WorkflowFailedEvent:
				event = &events.WorkflowFailed{}
			case events.NodeActivationEvent:
				event = &events.NodeActivation{}
			case events.NodeCompletionEvent:
				event = &events.NodeCompletion{}
			case events.NodeExecutionFinishedEvent:
				event = &events.NodeExecutionFinished{}
			case events.NodeExecutionFailedEvent:
				event = &events.NodeExecutionFailed{}
			case events.WorkflowExecutionStartedEvent:
				event = &events.WorkflowExecutionStarted{}
			case events.WorkflowExecutionCompletedEvent:
				event = &events.WorkflowExecutionCompleted{}
			case events.WorkflowExecutionFailedEvent:
				event = &events.WorkflowExecutionFailed{}
			case events.WorkflowExecutionCancelledEvent:
				event = &events.WorkflowExecutionCancelled{}
			case events.WorkflowExecutionTimeoutEvent:
				event = &events.WorkflowExecutionTimeout{}
			case events.WorkflowExecutionPausedEvent:
				event = &events.WorkflowExecutionPaused{}
			case events.WorkflowExecutionResumedEvent:
				event = &events.WorkflowExecutionResumed{}
			case events.WorkflowVariablesUpdatedEvent:
				event = &events.WorkflowVariablesUpdated{}
			case events.TriggerCreatedEventType:
				event = &events.TriggerCreatedEvent{}
			case events.TriggerUpdatedEventType:
				event = &events.TriggerUpdatedEvent{}
			case events.TriggerDeletedEventType:
				event = &events.TriggerDeletedEvent{}
			case events.WorkflowPublishedEventType:
				event = &events.WorkflowPublishedEvent{}
			case events.WorkflowUnpublishedEventType:
				event = &events.WorkflowUnpublishedEvent{}
			default:
				msg.Nack()

				continue
			}

			err := json.Unmarshal(msg.Payload, event)
			if err != nil {
				msg.Nack()

				continue
			}

			err = handler(ctx, event)
			if err != nil {
				msg.Nack()

				continue
			}

			msg.Ack()
		}
	}()

	return nil
}

func (eb *WatermillEventBus) Handle(eventType events.EventType, handler EventHandler) error {
	eb.subscriptions[eventType] = handler

	return nil
}

func (eb *WatermillEventBus) Close() error {
	err := eb.publisher.Close()
	if err != nil {
		return err
	}

	return eb.subscriber.Close()
}
