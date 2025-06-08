package app

import (
	"errors"
	"fmt"

	"github.com/hyuti/consumer-blueprint/config"
)

var ErrCfgEmpty = errors.New("config expected not to be empty")

func WithConfig() (*config.Config, error) {
	cfg, err := config.New()
	if err != nil {
		return nil, fmt.Errorf("cannot init config: %v", err)
	}
	return cfg, nil
}
func Cfg() *config.Config {
	mutex.Lock()
	defer mutex.Unlock()
	return app.cfg
}
