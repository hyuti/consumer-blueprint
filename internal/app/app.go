package app

import (
	"errors"
	"fmt"
	builtIn "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hyuti/Consumer-Golang-Template/config"
	"github.com/hyuti/Consumer-Golang-Template/pkg/kafka"
	"github.com/hyuti/Consumer-Golang-Template/pkg/logger"
	"github.com/hyuti/Consumer-Golang-Template/pkg/model"
	"github.com/hyuti/Consumer-Golang-Template/pkg/telegram"
	"golang.org/x/exp/slog"
	"sync"
)

const serviceKey = "service-name"

var (
	ErrLoggerEmpty = errors.New("logger expected not to be empty")
	ErrCfgEmpty    = errors.New("config expected not to be empty")
)
var (
	mutex sync.Mutex
	app   *App
)

type App struct {
	prod   *kafka.Producer
	cfg    *config.Config
	logger *slog.Logger
	tele   *telegram.Tele
}

func SharedKafkaConfigs(
	conf *builtIn.ConfigMap,
	username, password, protocol, mechanisms string) error {
	if username == "" || password == "" {
		return nil
	}
	err := conf.SetKey("sasl.username", username)
	if err != nil {
		return err
	}
	err = conf.SetKey("sasl.password", password)
	if err != nil {
		return err
	}
	err = conf.SetKey("security.protocol", protocol)
	if err != nil {
		return err
	}
	err = conf.SetKey("sasl.mechanisms", mechanisms)
	if err != nil {
		return err
	}
	return nil
}

func Init() error {
	mutex.Lock()
	defer mutex.Unlock()
	if app != nil {
		return nil
	}
	app = new(App)
	if err := WithConfig(); err != nil {
		return err
	}
	if err := WithLogger(); err != nil {
		return err
	}
	if err := WithTele(); err != nil {
		return err
	}
	if err := WithProd(); err != nil {
		return err
	}
	return nil
}

func WithProd() error {
	if app.cfg == nil {
		return ErrCfgEmpty
	}
	prod, err := kafka.NewProducer(app.cfg.Kafka.Broker, func(configMap *builtIn.ConfigMap) error {
		return SharedKafkaConfigs(configMap,
			app.cfg.Kafka.Username,
			app.cfg.Kafka.Password,
			app.cfg.Kafka.Protocol,
			app.cfg.Kafka.SaslMechanisms,
		)
	})
	if err != nil {
		return fmt.Errorf("%w :unable to init producer", err)
	}
	prod.RegisterTopic(model.Model{}.Name(), app.cfg.Kafka.Topic)
	app.prod = prod
	return nil
}

func WithConfig() error {
	cfg, err := config.New()
	if err != nil {
		return fmt.Errorf("%w :unable to init config", err)
	}
	app.cfg = cfg
	return nil
}

func WithLogger() error {
	if app.cfg == nil {
		return ErrCfgEmpty
	}
	app.logger = logger.FileAndStdLogger(
		app.cfg.FilePath,
		logger.WithLevelOpt(slog.LevelDebug),
	)
	app.logger = logger.WithServiceName(app.logger, serviceKey, app.cfg.App.Name)
	app.logger = logger.WithCtxID(app.logger)
	return nil
}

func WithTele() error {
	if app.cfg == nil {
		return ErrCfgEmpty
	}
	var err error
	app.tele, err = telegram.New(
		&telegram.TeleCfg{
			Token:        app.cfg.Telegram.Token,
			ChatID:       app.cfg.Telegram.ChatID,
			Debug:        app.cfg.App.Debug,
			FailSilently: app.cfg.Telegram.FailSilently,
		},
	)
	if err != nil {
		return fmt.Errorf("%w :unable to init telegram", err)
	}
	return nil
}
func Logger() *slog.Logger {
	mutex.Lock()
	defer mutex.Unlock()
	return app.logger
}
func Prod() *kafka.Producer {
	mutex.Lock()
	defer mutex.Unlock()
	return app.prod
}
func Tele() *telegram.Tele {
	mutex.Lock()
	defer mutex.Unlock()
	return app.tele
}
func Cfg() *config.Config {
	mutex.Lock()
	defer mutex.Unlock()
	return app.cfg
}
