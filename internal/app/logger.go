package app

import (
	"github.com/hyuti/consumer-blueprint/config"
	"github.com/hyuti/consumer-blueprint/pkg/logger"
	"golang.org/x/exp/slog"
)

//nolint:staticcheck // QF1008 prefer clear references
func WithLogger(cfg *config.Config) (*slog.Logger, error) {
	l := logger.FileAndStdLogger(
		cfg.FilePath,
		logger.WithLevelOpt(slog.LevelDebug),
	)
	l = logger.WithServiceName(l, serviceKey, cfg.App.Name)
	l = logger.WithCtxID(l)
	return l, nil
}

func Logger() *slog.Logger {
	mutex.Lock()
	defer mutex.Unlock()
	return app.logger
}
