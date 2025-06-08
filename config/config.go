package config

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
	"golang.org/x/exp/slog"
)

type (
	Config struct {
		Kafka `yaml:"KAFKA"`
		Log   `yaml:"LOG"`
		App   `yaml:"APP"`
	}
	App struct {
		Name   string `yaml:"NAME"`
		Debug  bool   `yaml:"DEBUG"`
		Worker int    `yaml:"WORKER"`
	}
	Kafka struct {
		Broker         string `yaml:"BROKER"`
		TopicRetry     string `yaml:"TOPIC_RETRY"`
		TopicDLQ       string `yaml:"TOPIC_DLQ"`
		Topic          string `yaml:"TOPIC"`
		Username       string `yaml:"USERNAME"`
		Password       string `yaml:"PASSWORD"`
		Protocol       string `yaml:"SECURITY_PROTOCOL"`
		SaslMechanisms string `yaml:"SASL_MECHANISMS"`
		Workers        int    `yaml:"WORKERS"`
	}
	Log struct {
		FilePath string     `yaml:"FILE_PATH"`
		Level    slog.Level `yaml:"LEVEL"`
	}
)

func New() (*Config, error) {
	cfg := new(Config)
	err := cleanenv.ReadConfig("./config/config.yaml", cfg)
	if err != nil {
		return nil, fmt.Errorf("config error: %w", err)
	}
	return cfg, nil
}
