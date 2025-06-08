package app

import (
	"sync"

	"github.com/hyuti/consumer-blueprint/config"
	"github.com/hyuti/consumer-blueprint/pkg/gru"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
	"golang.org/x/exp/slog"
)

const serviceKey = "service-name"

var (
	mutex sync.Mutex
	app   *App
)

// TODO: add compiler checker if a specific attribute not initilized but listed, same as one as the stringer pkg did
type App struct {
	prod   *kafka.Producer
	cfg    *config.Config
	logger *slog.Logger
	guru   *gru.Gru
	kafMan *kafka.Manager
}

func Init() error {
	mutex.Lock()
	defer mutex.Unlock()
	if app != nil {
		return nil
	}
	a, err := initializeApp()
	if err != nil {
		return err
	}
	app = a
	return nil
}
