package app

import (
	"fmt"

	builtIn "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hyuti/consumer-blueprint/config"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
)

func WithKafMan(cfg *config.Config) (*kafka.Manager, error) {
	c, err := kafka.NewManager(
		fmt.Sprintf("%v.consumer", cfg.App.Name),
		cfg.Kafka.Broker,
		func(cf *builtIn.ConfigMap) error {
			return SharedKafkaConfigs(cf,
				cfg.Kafka.Username,
				cfg.Kafka.Password,
				cfg.Kafka.Protocol,
				cfg.Kafka.SaslMechanisms)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot init consumer: %v", err)
	}
	c.WithWorker(cfg.Kafka.Workers)

	if cfg.Kafka.TopicRetry != "" {
		c.WithRetryTopic(cfg.Kafka.TopicRetry, 2)
	}
	if cfg.Kafka.TopicDLQ != "" {
		c.WithDLQTopic(cfg.Kafka.TopicDLQ)
	}
	return c, nil
}

func KafMan() *kafka.Manager {
	mutex.Lock()
	defer mutex.Unlock()
	return app.kafMan
}
