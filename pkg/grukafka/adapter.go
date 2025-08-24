package grukafka

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/hyuti/consumer-blueprint/pkg/gru"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
)

var _ gru.Broker = (*kafka.Manager)(nil)

func RunAdapter(guru chan *gru.Result, kafkaChan chan kafka.Result) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	for r := range kafkaChan {
		guru <- gru.NewResult(r.Ctx(), r.Msg(), r.Error(), r.Topic(), r.Value())
	}
	close(guru)
}
