package main

import (
	"github.com/hyuti/Consumer-Golang-Template/internal/app"
	"github.com/hyuti/Consumer-Golang-Template/internal/usecase"
	"github.com/hyuti/Consumer-Golang-Template/pkg/kafka"
	"log"
)

func registerTopics() {
	// register topics here
	h1 := usecase.NewUseCase(app.Logger())

	app.KafMan().RegisterTopic(app.Cfg().Kafka.Topic, kafka.NewFreeConsumerAdapter(h1))
}

func main() {
	if err := app.Init(); err != nil {
		log.Fatalln(err)
	}

	registerTopics()
	_ = app.Gru().Run()
}
