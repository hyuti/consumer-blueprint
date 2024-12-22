package app

import (
	"fmt"
	"github.com/hyuti/consumer-blueprint/config"
	"github.com/hyuti/consumer-blueprint/pkg/telegram"
)

func WithTele(cfg *config.Config) (*telegram.Tele, error) {
	t, err := telegram.New(
		&telegram.TeleCfg{
			Token:        cfg.Telegram.Token,
			ChatID:       cfg.Telegram.ChatID,
			Debug:        cfg.App.Debug,
			FailSilently: cfg.Telegram.FailSilently,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot init telegram: %v", err)
	}
	return t, nil
}
func Tele() *telegram.Tele {
	mutex.Lock()
	defer mutex.Unlock()
	return app.tele
}
