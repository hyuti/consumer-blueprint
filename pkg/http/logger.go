package http

import (
	"encoding/json"
	"golang.org/x/exp/slog"
)

type logger struct {
	*slog.Logger
}

func (l *logger) Write(p []byte) (n int, err error) {
	var msg map[string]any
	err = json.Unmarshal(p, &msg)
	if err != nil {
		return 0, err
	}
	l.Logger.Info("", "attrs", msg)
	return 0, nil
}
