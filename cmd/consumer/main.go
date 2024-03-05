package main

import (
	"errors"
	"fmt"
	builtIn "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hyuti/Consumer-Golang-Template/internal/app"
	"github.com/hyuti/Consumer-Golang-Template/internal/usecase"
	"github.com/hyuti/Consumer-Golang-Template/pkg/kafka"
	"github.com/hyuti/Consumer-Golang-Template/pkg/telegram"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func registerTopics(c *kafka.Manager) {
	// register topics here
	h1 := usecase.NewUseCase(app.Logger())

	c.RegisterTopic(app.Cfg().Kafka.Topic, kafka.NewFreeConsumerAdapter(h1))
}

var errConsumerStartFailed = errors.New("cannot start consumers")

func sharedKafkaConfigs(
	conf *builtIn.ConfigMap,
	username, password, protocol, mechanism string) error {
	return app.SharedKafkaConfigs(conf, username, password, protocol, mechanism)
}

func main() {
	if err := app.Init(); err != nil {
		log.Fatalln(err)
	}

	var wg sync.WaitGroup
	ch := make(chan kafka.Result, app.Cfg().Worker)
	for idx := 0; idx < app.Cfg().App.Worker; idx++ {
		c, err := kafka.NewManager(
			fmt.Sprintf("%v.consumer.%v", app.Cfg().App.Name, idx),
			app.Cfg().Kafka.Broker,
			func(cf *builtIn.ConfigMap) error {
				return sharedKafkaConfigs(cf,
					app.Cfg().Kafka.Username,
					app.Cfg().Kafka.Password,
					app.Cfg().Kafka.Protocol,
					app.Cfg().Kafka.SaslMechanisms)
			},
		)
		if err != nil {
			app.Logger().Error(fmt.Sprintf("%v: %v", errConsumerStartFailed, err))
			os.Exit(1)
		}
		c.WithWorker(app.Cfg().Kafka.Workers)
		registerTopics(c)

		c.WithChanResult(ch)
		c.WithProducer(app.Prod())
		if app.Cfg().Kafka.TopicRetry != "" {
			c.WithRetryTopic(app.Cfg().Kafka.TopicRetry, 2)
		}
		if app.Cfg().Kafka.TopicDLQ != "" {
			c.WithDLQTopic(app.Cfg().Kafka.TopicDLQ)
		}

		if err := c.SubscribeTopics(); err != nil {
			app.Logger().Error(fmt.Sprintf("%v: %v", errConsumerStartFailed, err))
			os.Exit(1)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Run()
		}()
	}

	app.Logger().Info("Start listening...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case result := <-ch:
			v := result.Value()
			if v == nil {
				v = string(result.Msg())
			}
			app.Logger().InfoContext(result.Ctx(),
				"consumed",
				"payload", v,
				"topic", result.Topic())
			if result.Error() == nil {
				continue
			}
			app.Logger().ErrorContext(
				result.Ctx(),
				"failed",
				"err", result.Error(),
				"topic", result.Topic(),
				"payload", v)

			go func() {
				e, ok := telegram.ConsiderErrShouldBeSent(result.Error(), app.Cfg().App.Name, result.Topic(), v)
				if !ok {
					return
				}
				_ = app.Tele().SendWithTeleMsg(e)
			}()
		case <-sigchan:
			close(ch)
			wg.Wait()
			return
		}
	}
}
