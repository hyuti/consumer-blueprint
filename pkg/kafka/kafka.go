package kafka

import (
	"context"
)

type (
	MsgNameType interface {
		Name() string
	}

	Result struct {
		ctx   context.Context
		err   error
		value any
		topic string
		chain string
		msg   []byte
	}
	MsgErr struct {
		Payload any    `json:"payload"`
		Err     string `json:"error"`
	}
)

var _ MsgNameType = (*MsgErr)(nil)

func (m MsgErr) Name() string {
	return "MsgErr"
}

func (s *Result) Error() error {
	return s.err
}

func (s *Result) Ctx() context.Context {
	return s.ctx
}
func (s *Result) Msg() []byte {
	return s.msg
}
func (s *Result) Topic() string {
	return s.topic
}
func (s *Result) Value() any {
	return s.value
}
func (s *Result) Chain() string {
	return s.chain
}
