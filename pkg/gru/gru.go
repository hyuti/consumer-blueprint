package gru

import (
	"golang.org/x/exp/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Broker interface {
	Run() error
}

type Gru struct {
	broker         Broker
	logger         *slog.Logger
	onErr          func(Result)
	ch             chan Result
	asyncBeforeRun []func()
}

func New(broker Broker) *Gru {
	l := slog.Default()
	return &Gru{
		broker: broker,
		logger: l,
	}
}

func (g *Gru) WithLogger(l *slog.Logger) {
	g.logger = l
}
func (g *Gru) WithOnErr(onErr func(Result)) {
	g.onErr = onErr
}
func (g *Gru) WithChanResult() chan Result {
	g.ch = make(chan Result)
	return g.ch
}
func (g *Gru) WithAsyncBeforeRun(runs ...func()) {
	g.asyncBeforeRun = append(g.asyncBeforeRun, runs...)
}

func (g *Gru) Run() error {
	if g.ch == nil {
		_ = g.WithChanResult()
	}
	if g.onErr == nil {
		g.onErr = func(_ Result) {}
	}

	var wg sync.WaitGroup

	wg.Add(len(g.asyncBeforeRun))
	for _, run := range g.asyncBeforeRun {
		go func(f func()) {
			defer wg.Done()
			f()
		}(run)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = g.broker.Run()
	}()

	g.logger.Info("Start listening...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for run := true; run; {
		select {
		case result := <-g.ch:
			v := result.Value()
			if v == nil {
				v = string(result.Msg())
			}
			g.logger.InfoContext(result.Ctx(),
				"consumed",
				"payload", v,
				"topic", result.Topic())
			if result.Error() == nil {
				continue
			}
			g.logger.ErrorContext(
				result.Ctx(),
				"failed",
				"err", result.Error(),
				"topic", result.Topic(),
				"payload", v)

			g.onErr(result)
		case <-sigchan:
			close(g.ch)
			wg.Wait()
			run = false
		}
	}
	return nil
}
