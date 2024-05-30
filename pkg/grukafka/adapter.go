package grukafka

import (
	"github.com/hyuti/Consumer-Golang-Template/pkg/gru"
	"github.com/hyuti/Consumer-Golang-Template/pkg/kafka"
	"os"
	"os/signal"
	"syscall"
)

func RunAdapter(guru chan gru.Result, kafka chan kafka.Result) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	for run := true; run; {
		select {
		case r := <-kafka:
			guru <- gru.NewResult(r.Ctx(), r.Msg(), r.Error(), r.Topic(), r.Value())
		case <-sigchan:
			run = false
		}
	}
}
