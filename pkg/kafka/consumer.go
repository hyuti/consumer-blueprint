package kafka

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pkgerr "github.com/hyuti/consumer-blueprint/pkg/error"
	"google.golang.org/protobuf/proto"
)

type (
	Consumer[T any] interface {
		Consume(ctx context.Context, msg T) error
	}

	JsonSerializer[T any] interface {
		Consumer[T]
		Serialize(ctx context.Context, rawValue []byte) (T, error)
	}

	ProtobufSerializer[T any] interface {
		Consumer[T]
		Serialize(ctx context.Context, rawValue []byte) (T, error)
	}

	JsonConsumer[T any] struct {
		handler Consumer[T]
	}

	ProtobufConsumer[T proto.Message] struct {
		handler Consumer[T]
	}

	payload struct {
		value           any
		topic           string
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
	return &JsonConsumer[T]{handler: handler}
}

func NewProtobufConsumer[T proto.Message](handler Consumer[T]) *ProtobufConsumer[T] {
	return &ProtobufConsumer[T]{handler: handler}
}

var _ Consumer[[]byte] = (*JsonConsumer[any])(nil)

func (c *JsonConsumer[T]) Consume(ctx context.Context, msg []byte) error {
	value, err := c.Serialize(ctx, msg)
	if err != nil {
		return err
	}

	if err = c.handler.Consume(ctx, value); err != nil {
		return err
	}
	return nil
}

func (c *JsonConsumer[T]) Serialize(ctx context.Context, rawValue []byte) (t T, err error) {
	err = json.Unmarshal(rawValue, &t)
	return t, err
}

func (c *ProtobufConsumer[T]) Consume(ctx context.Context, msg []byte) error {
	value, err := c.Serialize(ctx, msg)
	if err != nil {
		err = pkgerr.ErrInternalServer(err,
			pkgerr.WithPayloadOpt(msg),
			pkgerr.WithChainOpt("c.Serialize"),
			pkgerr.ErrFuncTriggerOpt("c.Serialize"))
		return err
	}

	if err = c.handler.Consume(ctx, value); err != nil {
		if errors.As(err, new(pkgerr.Error)) {
			return err
		}
		err = pkgerr.ErrInternalServer(err,
			pkgerr.WithPayloadOpt(value),
			pkgerr.WithChainOpt("c.handler.Consume"),
			pkgerr.ErrFuncTriggerOpt("c.handler.Consume"))

		return err
	}
	return nil
}

func (c *ProtobufConsumer[T]) Serialize(ctx context.Context, rawValue []byte) (t T, err error) {
	return t, proto.Unmarshal(rawValue, t)
}
