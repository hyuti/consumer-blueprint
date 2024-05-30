package gru

import "context"

type Result struct {
	ctx   context.Context
	err   error
	msg   []byte
	topic string
	value any
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
func NewResult(ctx context.Context, msg []byte, err error, topic string, value any) Result {
	return Result{
		ctx:   ctx,
		err:   err,
		msg:   msg,
		topic: topic,
		value: value,
	}
}
