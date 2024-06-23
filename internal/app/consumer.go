package app

import (
	"errors"
	"fmt"
	builtIn "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
)

var ErrKafkaEmpty = errors.New("kafka expected not to be empty")

func WithKafMan() (*kafka.Manager, error) {
	if app.cfg == nil {
		return nil, ErrCfgEmpty
	}
	if app.prod == nil {
		return nil, ErrProdEmpty
	}

	c, err := kafka.NewManager(
		fmt.Sprintf("%v.consumer", Cfg().App.Name),
		Cfg().Kafka.Broker,
		func(cf *builtIn.ConfigMap) error {
			return SharedKafkaConfigs(cf,
				Cfg().Kafka.Username,
				Cfg().Kafka.Password,
				Cfg().Kafka.Protocol,
				Cfg().Kafka.SaslMechanisms)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot init consumer: %v", err)
	}
	c.WithWorker(app.cfg.Kafka.Workers)

	c.WithProducer(app.prod)
	if app.cfg.Kafka.TopicRetry != "" {
		c.WithRetryTopic(app.cfg.Kafka.TopicRetry, 2)
	}
	if app.cfg.Kafka.TopicDLQ != "" {
		c.WithDLQTopic(app.cfg.Kafka.TopicDLQ)
	}
	return c, nil
}
func KafMan() *kafka.Manager {
	mutex.Lock()
	defer mutex.Unlock()
	return app.kafMan
}
