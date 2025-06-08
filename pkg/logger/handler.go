package logger

import (
	"context"

	"golang.org/x/exp/slog"
)

type ctxHandler struct {
	slog.Handler
	attrs []func(ctx context.Context) slog.Attr
}

var _ slog.Handler = (*ctxHandler)(nil)

//nolint:gocritic // 3rd party package
func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	for idx := range h.attrs {
		attr := h.attrs[idx](ctx)
		r.AddAttrs(attr)
	}
	return h.Handler.Handle(ctx, r)
}
