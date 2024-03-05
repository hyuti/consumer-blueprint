package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type retryConsumer struct {
	handlers map[string]Consumer[*payload]
	retries  int
}
type MsgRetry struct {
	Topic   string `json:"topic"`
	Payload any    `json:"payload"`
}

var _ MsgNameType = (*MsgRetry)(nil)

func (m MsgRetry) Name() string {
	return "MsgRetry"
}

var _ Consumer[*MsgRetry] = (*retryConsumer)(nil)

func NewRetryConsumer(h map[string]Consumer[*payload], r int) Consumer[*MsgRetry] {
	return &retryConsumer{
		handlers: h,
		retries:  r,
	}
}

func (c *retryConsumer) Consume(ctx context.Context, msg *MsgRetry) error {
	h, ok := c.handlers[msg.Topic]
	if !ok {
		return fmt.Errorf("unable to find handler for %v topic", msg.Topic)
	}
	v, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	retries := 1
	if c.retries > 1 {
		retries = c.retries
	}
	errs := make([]error, 0, retries)
	for retry := 0; retry <= retries; retry += 1 {
		err = h.Consume(ctx, &payload{
			rawValue: v,
			topic:    msg.Topic,
		})
		if err == nil {
			return nil
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
