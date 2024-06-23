package app

import (
	"errors"
	"fmt"
	"github.com/hyuti/consumer-blueprint/pkg/telegram"
)

var ErrTeleEmpty = errors.New("telegram expected not to be empty")

func WithTele() (*telegram.Tele, error) {
	if app.cfg == nil {
		return nil, ErrCfgEmpty
	}
	t, err := telegram.New(
		&telegram.TeleCfg{
			Token:        app.cfg.Telegram.Token,
			ChatID:       app.cfg.Telegram.ChatID,
			Debug:        app.cfg.App.Debug,
			FailSilently: app.cfg.Telegram.FailSilently,
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
