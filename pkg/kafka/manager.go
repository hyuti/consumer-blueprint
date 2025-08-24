package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	ctxPkg "github.com/hyuti/consumer-blueprint/pkg/ctx"
	pkgerr "github.com/hyuti/consumer-blueprint/pkg/error"
)

type Manager struct {
	workerPool            sync.Pool
	reader                *kafka.Consumer
	workerCounter         chan struct{}
	bus                   chan *payload
	handlers              map[string]Consumer[[]byte]
	deadLetterQueueTopics map[string]string
	writer                *Producer
	chanResult            chan Result
	workerTable           map[string]*workerInfo
	groupID               string
	prefixPath            string
	topics                []string
	wg                    sync.WaitGroup
	retries               int
	workers               int
	workerTimeout         time.Duration
}

func NewManager(
	groupID,
	broker string,
	opts ...func(*kafka.ConfigMap) error) (*Manager, error) {
	cfg := &kafka.ConfigMap{
		"group.id":             groupID,
		"auto.offset.reset":    "latest",
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
	_, filePath, _, _ := runtime.Caller(0)
	filePath = filePath[:len(filePath)-len("pkg/kafka/manager.go")]

	m := &Manager{
		prefixPath:            filePath,
		reader:                c,
		groupID:               groupID,
		workers:               100,
		handlers:              make(map[string]Consumer[[]byte], 1),
		deadLetterQueueTopics: make(map[string]string, 1),
		workerPool: sync.Pool{New: func() any {
			return new(workerInfo)
		}},
	}
	return m, nil
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

func (s *Manager) WithWorkerTimeout(timeout time.Duration) {
	s.workerTimeout = timeout
}

func (s *Manager) WithChanResult() chan Result {
	s.chanResult = make(chan Result, 1)
	return s.chanResult
}

func (s *Manager) RegisterTopic(topic string, handler Consumer[[]byte], deadLetterQueueTopic ...string) {
	if _, ok := s.handlers[topic]; !ok {
		s.topics = append(s.topics, topic)
	}
	if len(deadLetterQueueTopic) > 0 {
		s.deadLetterQueueTopics[topic] = deadLetterQueueTopic[0]
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
	if s.workerTimeout == 0 {
		s.workerTimeout = time.Minute
	}
	s.bus = make(chan *payload, 1)
	s.workerCounter = make(chan struct{}, s.workers)
	s.workerTable = make(map[string]*workerInfo, s.workers)

	s.populateWorkers()
	defer s.close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	p := payload{}
	for run := true; run; {
		select {
		case <-sigchan:
			run = false
		default:
			m, err := s.reader.ReadMessage(100)
			if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue
			}
			c := ctxPkg.WithCtxID(ctxPkg.New())
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
			p.rawValue = m.Value
			p.topic = *m.TopicPartition.Topic
			p.deadLetterQueue = s.deadLetterQueueTopics[p.topic]
			s.bus <- &p
		}
	}
	return nil
}

type workerInfo struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	ctxID      string
	topic      string
	rawValue   []byte
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
				s.workerCounter <- struct{}{}

				c := ctxPkg.WithCtxID(ctxPkg.New())
				c, cancel := context.WithTimeout(c, s.workerTimeout)
				info := s.workerPool.Get().(*workerInfo)
				info.ctx = c
				info.rawValue = pl.rawValue
				info.topic = pl.topic
				info.ctxID = ctxPkg.GetCtxID(c)
				info.cancelFunc = cancel
				s.workerTable[info.ctxID] = info
				s.wg.Add(1)
				go func(rawValue []byte, topic, deadLetterQueue string) {
					defer func() {
						cancel()
						s.wg.Done()
						s.workerPool.Put(info)
						delete(s.workerTable, info.ctxID)
						<-s.workerCounter
					}()
					err := s.recoverIfPanic(c, info)
					if err == nil {
						s.chanResult <- Result{
							err:   nil,
							ctx:   c,
							msg:   rawValue,
							topic: topic,
						}
						return
					}
					var pl any
					var chain string
					if plErr := new(pkgerr.Error); errors.As(err, &plErr) {
						pl = plErr.Payload()
						chain = plErr.Chain()
					}
					s.chanResult <- Result{
						err:   err,
						ctx:   c,
						msg:   rawValue,
						topic: topic,
						value: pl,
						chain: chain,
					}

					if deadLetterQueue != "" && s.writer != nil {
						_ = s.writer.ProduceBytes(rawValue, deadLetterQueue)
					}
				}(pl.rawValue, pl.topic, pl.deadLetterQueue)
			}
		}
	}
	s.wg.Add(1)
	go exe()
}

func (s *Manager) retryIfFail(ctx context.Context, info *workerInfo) error {
	retries := 0
	if s.retries > 0 {
		retries = s.retries
	}
	errs := make([]error, 0, retries+1)
	for retry := 0; retry <= retries; retry += 1 {
		handler, ok := s.handlers[info.topic]
		if !ok {
			return fmt.Errorf("unable to find handler for %s topic", info.topic)
		}
		err := handler.Consume(ctx, info.rawValue)
		if err == nil {
			return nil
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *Manager) recoverIfPanic(ctx context.Context, info *workerInfo) (errConsumer error) {
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
			_, file, line, ok := runtime.Caller(skip)
			if !ok {
				break
			}
			if len(file) > len(s.prefixPath) {
				file = file[len(s.prefixPath):]
			}
			chain = append(chain, fmt.Sprintf(
				"%s:%d",
				file,
				line,
			))
		}
		errConsumer = pkgerr.ErrInternalServer(err, pkgerr.WithChainOpt(chain...))
	}()
	errConsumer = s.retryIfFail(ctx, info)
	return errConsumer
}

func (s *Manager) close() error {
	for _, worker := range s.workerTable {
		worker.cancelFunc()
	}
	s.wg.Wait()

	if s.bus != nil {
		close(s.bus)
	}
	if s.workerCounter != nil {
		close(s.workerCounter)
	}
	if s.chanResult != nil {
		close(s.chanResult)
	}
	if s.writer != nil {
		s.writer.Close()
	}
	_ = s.reader.Close()
	return nil
}
