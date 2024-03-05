package kafka

import (
	"context"
)

type (
	MsgFrame[T any] struct {
		Payload T `json:"payload"`
	}
	MsgNameType interface {
		Name() string
	}

	Result struct {
		ctx   context.Context
		err   error
		msg   []byte
		topic string
		value any
	}
	MsgErr struct {
		Err     string `json:"error"`
		Payload any    `json:"payload"`
	}
)

var _ MsgNameType = (*MsgErr)(nil)

func (m MsgErr) Name() string {
	return "MsgErr"
}

func (s Result) Error() error {
	return s.err
}

func (s Result) Ctx() context.Context {
	return s.ctx
}
func (s Result) Msg() []byte {
	return s.msg
}
func (s Result) Topic() string {
	return s.topic
}
func (s Result) Value() any {
	return s.value
}
