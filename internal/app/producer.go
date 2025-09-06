package app

import (
	"fmt"

	builtIn "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hyuti/consumer-blueprint/config"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
	"github.com/hyuti/consumer-blueprint/pkg/model"
)

func WithProd(cfg *config.Config) (*kafka.Producer, error) {
	prod, err := kafka.NewProducer(cfg.Kafka.Broker, func(configMap *builtIn.ConfigMap) error {
		return sharedKafkaConfigs(configMap,
			cfg.Kafka.Username,
			cfg.Kafka.Password,
			cfg.Kafka.Protocol,
			cfg.Kafka.SaslMechanisms,
		)
	})
	if err != nil {
		return nil, fmt.Errorf("cannot init producer: %v", err)
	}
	prod.RegisterTopic(model.Model{}.Name(), cfg.Kafka.Topic)
	return prod, nil
}

func Prod() *kafka.Producer {
	mutex.Lock()
	defer mutex.Unlock()
	return app.prod
}

func sharedKafkaConfigs(
	conf *builtIn.ConfigMap,
	username, password, protocol, mechanisms string) error {
	if username == "" || password == "" {
		return nil
	}
	err := conf.SetKey("sasl.username", username)
	if err != nil {
		return err
	}
	err = conf.SetKey("sasl.password", password)
	if err != nil {
		return err
	}
	err = conf.SetKey("security.protocol", protocol)
	if err != nil {
		return err
	}
	err = conf.SetKey("sasl.mechanisms", mechanisms)
	if err != nil {
		return err
	}
	return nil
}
