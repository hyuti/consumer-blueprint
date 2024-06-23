package app

import (
	"errors"
	"github.com/hyuti/consumer-blueprint/pkg/logger"
	"golang.org/x/exp/slog"
)

var ErrLoggerEmpty = errors.New("logger expected not to be empty")

func WithLogger() (*slog.Logger, error) {
	if app.cfg == nil {
		return nil, ErrCfgEmpty
	}
	l := logger.FileAndStdLogger(
		app.cfg.FilePath,
		logger.WithLevelOpt(slog.LevelDebug),
	)
	l = logger.WithServiceName(l, serviceKey, app.cfg.App.Name)
	l = logger.WithCtxID(l)
	return l, nil
}
func Logger() *slog.Logger {
	mutex.Lock()
	defer mutex.Unlock()
	return app.logger
}
