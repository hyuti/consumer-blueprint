package app

import (
	"github.com/hyuti/consumer-blueprint/pkg/gru"
	"github.com/hyuti/consumer-blueprint/pkg/grukafka"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
	"golang.org/x/exp/slog"
)

func WithGru(
	kafMan *kafka.Manager,
	logger *slog.Logger,
) (*gru.Gru, error) {
	guru := gru.New(kafMan)
	guru.WithLogger(logger)
	guru.WithAsyncBeforeRun(func() {
		grukafka.RunAdapter(guru.WithChanResult(), kafMan.WithChanResult())
	})
	return guru, nil
}
func Gru() *gru.Gru {
	mutex.Lock()
	defer mutex.Unlock()
	return app.guru
}
