package main

import (
	"log"

	"github.com/hyuti/consumer-blueprint/internal/app"
	"github.com/hyuti/consumer-blueprint/internal/usecase"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
)

func init() {
	if err := app.Init(); err != nil {
		log.Fatalln(err)
	}
}

//nolint:staticcheck // QF1008 prefer clear references
func registerTopics() {
	// register topics here
	h1 := usecase.NewUseCase(app.Logger())

	registra := app.KafMan()

	registra.RegisterTopic(app.Cfg().Kafka.Topic, kafka.NewFreeConsumerAdapter(h1))
}

func main() {
	registerTopics()

	if err := app.Gru().Run(); err != nil {
		log.Fatalln(err)
	}
}
