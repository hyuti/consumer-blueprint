package gru

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"golang.org/x/exp/slog"
)

type Broker interface {
	Run() error
}

type Gru struct {
	broker         Broker
	logger         *slog.Logger
	onErr          func(*Result)
	ch             chan *Result
	asyncBeforeRun []func()
}

func New(broker Broker) *Gru {
	return &Gru{
		broker: broker,
	}
}

func (g *Gru) WithLogger(l *slog.Logger) {
	g.logger = l
}
func (g *Gru) WithOnErr(onErr func(*Result)) {
	g.onErr = onErr
}
func (g *Gru) WithChanResult() chan *Result {
	g.ch = make(chan *Result)
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
		g.onErr = func(_ *Result) {}
	}
	if g.logger == nil {
		g.logger = slog.Default()
	}

	var wg sync.WaitGroup

	wg.Add(len(g.asyncBeforeRun))
	for idx := range g.asyncBeforeRun {
		runner := g.asyncBeforeRun[idx]
		go func() {
			defer wg.Done()
			runner()
		}()
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
			if result.Error() != nil {
				g.logger.ErrorContext(
					result.Ctx(),
					result.Error().Error(),
					"topic", result.Topic(),
					"chain", result.Chain(),
					"payload", v,
				)
				g.onErr(result)
				continue
			}
			g.logger.InfoContext(result.Ctx(),
				"success",
				"payload", v,
				"topic", result.Topic())
		case <-sigchan:
			close(g.ch)
			wg.Wait()
			run = false
		}
	}
	return nil
}

// RunWeak depends on callers to shut down. See Run for completely controlling shut down.
func (g *Gru) RunWeak() error {
	if g.ch == nil {
		_ = g.WithChanResult()
	}
	if g.onErr == nil {
		g.onErr = func(_ *Result) {}
	}
	if g.logger == nil {
		g.logger = slog.Default()
	}

	var wg sync.WaitGroup
	wg.Add(len(g.asyncBeforeRun))
	for idx := range g.asyncBeforeRun {
		runner := g.asyncBeforeRun[idx]
		go func() {
			defer wg.Done()
			runner()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = g.broker.Run()
	}()

	g.logger.Info("Start listening...")
	for result := range g.ch {
		v := result.Value()
		if v == nil {
			v = string(result.Msg())
		}
		if result.Error() != nil {
			g.logger.ErrorContext(
				result.Ctx(),
				result.Error().Error(),
				"topic", result.Topic(),
				"chain", result.Chain(),
				"payload", v,
			)
			g.onErr(result)
			continue
		}
		g.logger.InfoContext(result.Ctx(),
			"success",
			"payload", v,
			"topic", result.Topic())
	}
	wg.Wait()
	return nil
}
