package kafka

import (
	"context"
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	Consumer[T any] interface {
		Consume(ctx context.Context, msg T) error
	}

	JsonSerializer[T any] interface {
		Consumer[T]
		Serialize(ctx context.Context, rawValue []byte) (T, error)
	}

	JsonConsumer[T any] struct {
		handler Consumer[T]
	}

	payload struct {
		value           any
		topic           string
		retry           string
		deadLetterQueue string
		rawValue        []byte
	}
)

func HealthCheck(url string) error {
	config := &kafka.ConfigMap{
		"bootstrap.servers": url,
	}

	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	topics, err := adminClient.GetMetadata(nil, true, -1)
	if err != nil {
		return err
	}

	if len(topics.Topics) == 0 {
		return err
	}
	return nil
}

func NewJsonConsumer[T any](handler Consumer[T]) *JsonConsumer[T] {
	return &JsonConsumer[T]{
		handler: handler,
	}
}

var _ Consumer[[]byte] = (*JsonConsumer[any])(nil)

func (c *JsonConsumer[T]) Consume(ctx context.Context, msg []byte) error {
	value, err := c.Serialize(ctx, msg)
	if err != nil {
		return err
	}
	return c.handler.Consume(ctx, value)
}

func (c *JsonConsumer[T]) Serialize(ctx context.Context, rawValue []byte) (t T, err error) {
	err = json.Unmarshal(rawValue, &t)
	return t, err
}
