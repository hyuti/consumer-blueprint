package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hyuti/consumer-blueprint/pkg/ctx"
	pkgerr "github.com/hyuti/consumer-blueprint/pkg/error"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

type Manager struct {
	writer     *Producer
	reader     *kafka.Consumer
	groupID    string
	workers    int
	retries    int
	handlers   map[string]Consumer[*payload]
	topics     []string
	wg         sync.WaitGroup
	retryTopic *string
	dlqTopic   *string
	chanResult chan Result
	bus        chan *payload
	workerPool chan struct{}
}

func NewManager(
	groupID,
	broker string,
	opts ...func(*kafka.ConfigMap) error) (*Manager, error) {
	cfg := &kafka.ConfigMap{
		"group.id":             groupID,
		"auto.offset.reset":    "earliest",
		"bootstrap.servers":    broker,
		"max.poll.interval.ms": 600000,
	}
	for _, opt := range opts {
		err := opt(cfg)
		if err != nil {
			return nil, err
		}
	}
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		reader:   c,
		groupID:  groupID,
		workers:  10000,
		handlers: map[string]Consumer[*payload]{},
	}

	return m, nil
}
func (s *Manager) WithRetryTopic(t string, retries int) {
	h := NewRetryConsumer(s.handlers, retries)
	s.RegisterTopic(t, NewConsumerAdapter(h))
	s.retryTopic = &t
	s.retries = 0

	if s.writer != nil {
		s.writer.RegisterTopic(MsgRetry{}.Name(), t)
	}
}

// WithDLQTopic Register DeadLetterQueue topic
func (s *Manager) WithDLQTopic(t string) {
	if s.writer == nil {
		return
	}
	s.dlqTopic = &t
	s.writer.RegisterTopic(MsgErr{}.Name(), t)
}

func (s *Manager) WithProducer(p *Producer) {
	s.writer = p
}

func (s *Manager) WithWorker(w int) {
	s.workers = w
}

func (s *Manager) WithRetries(w int) {
	s.retries = w
}

func (s *Manager) WithChanResult() chan Result {
	s.chanResult = make(chan Result)
	return s.chanResult
}

func (s *Manager) RegisterTopic(topic string, handler Consumer[*payload]) {
	_, ok := s.handlers[topic]
	if !ok {
		s.topics = append(s.topics, topic)
	}
	s.handlers[topic] = handler
}

func (s *Manager) SubscribeTopics() error {
	return s.reader.SubscribeTopics(s.topics, nil)
}

func (s *Manager) Run() error {
	if err := s.SubscribeTopics(); err != nil {
		return err
	}

	if s.chanResult == nil {
		_ = s.WithChanResult()
	}
	s.bus = make(chan *payload, 1)
	s.workerPool = make(chan struct{}, s.workers)

	s.populateWorkers()
	defer s.close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for run := true; run; {
		select {
		case <-sigchan:
			run = false
		default:
			m, err := s.reader.ReadMessage(100)
			if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue
			}
			c := ctx.WithCtxID(ctx.New())
			if err != nil {
				s.chanResult <- Result{
					err: fmt.Errorf("%s: %w", s.groupID, err),
					ctx: c,
				}
				continue
			}
			if m.TopicPartition.Topic == nil {
				s.chanResult <- Result{
					err: errors.New("topic expected not to be empty"),
					ctx: c,
				}
				continue
			}
			p := payload{
				rawValue: m.Value,
				topic:    *m.TopicPartition.Topic,
			}
			s.bus <- &p
		}
	}
	s.wg.Wait()
	return nil
}
func (s *Manager) populateWorkers() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	exe := func() {
		defer s.wg.Done()
		for {
			select {
			case <-sigchan:
				return
			case pl := <-s.bus:
				s.workerPool <- struct{}{}
				s.wg.Add(1)
				go func(m *payload) {
					defer func() {
						s.wg.Done()
						<-s.workerPool
					}()
					c := ctx.WithCtxID(ctx.New())
					err := s.recoverIfPanic(c, m)
					if err == nil {
						s.chanResult <- Result{
							err:   nil,
							ctx:   c,
							msg:   m.rawValue,
							topic: m.topic,
							value: m.value,
						}
						return
					}
					s.chanResult <- Result{
						err:   err,
						ctx:   c,
						msg:   m.rawValue,
						topic: m.topic,
						value: m.value,
					}
					retry := m.topic
					if s.retryTopic != nil {
						retry = *s.retryTopic
					}

					if retry != m.topic {
						if m.value == nil || s.writer == nil {
							return
						}

						_ = s.writer.Produce(MsgRetry{
							Topic:   m.topic,
							Payload: m.value,
						})
						return
					}

					if s.dlqTopic != nil {
						if m.value == nil || s.writer == nil {
							return
						}

						_ = s.writer.Produce(MsgErr{
							Payload: m.value,
							Err:     err.Error(),
						})
						return
					}
				}(pl)
			}
		}
	}
	s.wg.Add(1)
	go exe()
}

func (s *Manager) retryIfFail(ctx context.Context, msg *payload) error {
	retries := 0
	if s.retries > 0 {
		retries = s.retries
	}
	errs := make([]error, 0, retries)
	for retry := 0; retry <= retries; retry += 1 {
		handler, ok := s.handlers[msg.topic]
		if !ok {
			return fmt.Errorf("unable to find handler for %v topic", msg.topic)
		}
		err := handler.Consume(ctx, msg)
		if err == nil {
			return nil
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *Manager) recoverIfPanic(ctx context.Context, msg *payload) (errConsumer error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err, ok := r.(error)
		if !ok {
			err = errors.New("error internal server")
			errMsg, ok := r.(string)
			if ok {
				err = errors.New(errMsg)
			}
		}
		chain := make([]string, 0, 2)
		for skip := 2; skip < 4; skip += 1 {
			pc, file, line, ok := runtime.Caller(skip)
			if !ok {
				break
			}
			chain = append(chain, fmt.Sprintf(
				"%s (%s:%d)",
				runtime.FuncForPC(pc).Name(),
				file,
				line,
			))
		}
		errConsumer = pkgerr.ErrInternalServer(err, pkgerr.WithChainOpt(chain...))
	}()
	errConsumer = s.retryIfFail(ctx, msg)
	return errConsumer
}

func (s *Manager) close() error {
	if s.bus != nil {
		close(s.bus)
	}
	if s.workerPool != nil {
		close(s.workerPool)
	}
	return s.reader.Close()
}
