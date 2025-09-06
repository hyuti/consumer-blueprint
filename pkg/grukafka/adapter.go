package grukafka

import (
	"github.com/hyuti/consumer-blueprint/pkg/gru"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
)

var _ gru.Broker = (*kafka.Manager)(nil)

func RunAdapter(guru chan *gru.Result, kafkaChan chan kafka.Result) {
	for r := range kafkaChan {
		guru <- gru.NewResult(r.Ctx(), r.Msg(), r.Error(), r.Topic(), r.Chain(), r.Value())
	}
	close(guru)
}
