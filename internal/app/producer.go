package app

import (
	"errors"
	"fmt"
	builtIn "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
	"github.com/hyuti/consumer-blueprint/pkg/model"
)

var ErrProdEmpty = errors.New("producer expected not to be empty")

func WithProd() (*kafka.Producer, error) {
	if app.cfg == nil {
		return nil, ErrCfgEmpty
	}
	prod, err := kafka.NewProducer(app.cfg.Kafka.Broker, func(configMap *builtIn.ConfigMap) error {
		return SharedKafkaConfigs(configMap,
			app.cfg.Kafka.Username,
			app.cfg.Kafka.Password,
			app.cfg.Kafka.Protocol,
			app.cfg.Kafka.SaslMechanisms,
		)
	})
	if err != nil {
		return nil, fmt.Errorf("cannot init producer: %v", err)
	}
	prod.RegisterTopic(model.Model{}.Name(), app.cfg.Kafka.Topic)
	return prod, nil
}
func Prod() *kafka.Producer {
	mutex.Lock()
	defer mutex.Unlock()
	return app.prod
}

func SharedKafkaConfigs(
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
