package kafka

import (
	"context"
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hyuti/consumer-blueprint/pkg/tool"
)

type (
	Consumer[T any] interface {
		Consume(ctx context.Context, msg T) error
	}
	consumer[T any] struct {
		handler Consumer[T]
	}
	freeConsumer[T any] struct {
		handler Consumer[T]
	}
	payload struct {
		value    any
		topic    string
		rawValue []byte
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
func NewFreeConsumerAdapter[T any](handler Consumer[T]) Consumer[*payload] {
	return &freeConsumer[T]{
		handler: handler,
	}
}

func NewConsumerAdapter[T any](handler Consumer[T]) Consumer[*payload] {
	return &consumer[T]{
		handler: handler,
	}
}

var _ Consumer[*payload] = (*consumer[any])(nil)
var _ Consumer[*payload] = (*freeConsumer[any])(nil)

func (c *consumer[T]) Consume(ctx context.Context, msg *payload) error {
	var t MsgFrame[T]

	if err := json.Unmarshal(msg.rawValue, &t); err != nil {
		return err
	}
	if tool.IsNil(t.Payload) {
		return nil
	}
	msg.value = t.Payload
	return c.handler.Consume(ctx, t.Payload)
}

func (c *freeConsumer[T]) Consume(ctx context.Context, msg *payload) error {
	var t T

	err := json.Unmarshal(msg.rawValue, &t)
	if err != nil {
		return err
	}
	msg.value = t
	return c.handler.Consume(ctx, t)
}
