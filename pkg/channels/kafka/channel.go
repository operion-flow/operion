// Package kafka provides Apache Kafka integration for event messaging.
package kafka

import (
	"errors"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dukex/operion/pkg/events"
)

var (
	errMessageNil           = errors.New("message is nil")
	errMessageMetadataNil   = errors.New("message metadata is nil")
	errMessageMetadataNoKey = errors.New("message metadata does not contain 'key'")
	errKafkaBrokersNotSet   = errors.New("KAFKA_BROKERS environment variable is not set or empty")
)

func MetadataKey(topic string, msg *message.Message) (string, error) {
	if msg == nil {
		return "", errMessageNil
	}

	if msg.Metadata == nil || msg.Metadata[events.EventMetadataKey] == "" {
		return "", errMessageMetadataNil
	}

	key, ok := msg.Metadata[events.EventMetadataKey]
	if !ok || key == "" {
		return "", errMessageMetadataNoKey
	}

	return key, nil
}

func CreateChannel(logger watermill.LoggerAdapter, serviceName string) (*kafka.Publisher, *kafka.Subscriber, error) {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if len(brokers) == 0 || brokers[0] == "" {
		return nil, nil, errKafkaBrokersNotSet
	}

	saramaSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaSubscriberConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               brokers,
			Unmarshaler:           kafka.NewWithPartitioningMarshaler(MetadataKey),
			OverwriteSaramaConfig: saramaSubscriberConfig,
			ConsumerGroup:         "cg-" + serviceName,
			Tracer:                kafka.NewOTELSaramaTracer(),
		},
		logger,
	)
	if err != nil {
		return nil, nil, err
	}

	saramaPublisherConfig := sarama.NewConfig()
	saramaPublisherConfig.Producer.Return.Successes = true

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               brokers,
			Marshaler:             kafka.NewWithPartitioningMarshaler(MetadataKey),
			OverwriteSaramaConfig: saramaPublisherConfig,
			Tracer:                kafka.NewOTELSaramaTracer(),
		},
		logger,
	)
	if err != nil {
		return nil, nil, err
	}

	return publisher, subscriber, nil
}
