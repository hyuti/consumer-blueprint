package usecase

import (
	"context"
	"github.com/hyuti/consumer-blueprint/pkg/kafka"
	"github.com/hyuti/consumer-blueprint/pkg/model"
	"golang.org/x/exp/slog"
)

type uc struct {
	log *slog.Logger
}

var _ kafka.Consumer[*model.Model] = (*uc)(nil)

func (u *uc) Consume(ctx context.Context, msg *model.Model) error {
	panic("implement me")
	return nil
}

func NewUseCase(
	log *slog.Logger,
) kafka.Consumer[*model.Model] {
	return &uc{
		log: log,
	}
}
