package app

import (
	"github.com/hyuti/consumer-blueprint/pkg/gru"
	"github.com/hyuti/consumer-blueprint/pkg/grukafka"
	"github.com/hyuti/consumer-blueprint/pkg/telegram"
)

func WithGru() (*gru.Gru, error) {
	if app.cfg == nil {
		return nil, ErrCfgEmpty
	}
	if app.kafMan == nil {
		return nil, ErrKafkaEmpty
	}
	if app.tele == nil {
		return nil, ErrTeleEmpty
	}
	if app.logger == nil {
		return nil, ErrLoggerEmpty
	}

	guru := gru.New(app.kafMan)
	guru.WithLogger(app.logger)
	guru.WithAsyncBeforeRun(func() {
		grukafka.RunAdapter(guru.WithChanResult(), app.kafMan.WithChanResult())
	})
	guru.WithOnErr(func(result gru.Result) {
		go func() {
			e, ok := telegram.ConsiderErrShouldBeSent(result.Error(), Cfg().App.Name, result.Topic(), result.Value())
			if !ok {
				return
			}
			_ = Tele().SendWithTeleMsg(e)
		}()
	})
	return guru, nil
}

func Gru() *gru.Gru {
	mutex.Lock()
	defer mutex.Unlock()
	return app.guru
}
