package tool

import (
	"errors"
	"runtime/debug"
	"sync"
)

// Worker running multiple tasks concurrently by taking advantage of goroutines
// A tool to reduce boilerplate of handling goroutines.
type Worker[T any] struct {
	jobs       []func()
	eventJob   func(int)
	errHandler func(error)
	wg         sync.WaitGroup
	ch         chan T
	errCh      chan error
	errs       []error
}

func New[T any]() *Worker[T] {
	w := new(Worker[T])
	return w
}

func (w *Worker[T]) AddJob(job func() T) *Worker[T] {
	wrapper := func() {
		defer func() {
			if r := recover(); r != nil {
				if w.errCh == nil {
					return
				}
				w.errCh <- errors.New(string(debug.Stack()))
			}
		}()
		t := job()
		if w.ch != nil {
			w.ch <- t
		}
	}
	w.jobs = append(w.jobs, wrapper)
	return w
}

func (w *Worker[T]) Start() {
	for idx := range w.jobs {
		w.wg.Add(1)
		go func(_idx int) {
			defer w.wg.Done()
			w.jobs[_idx]()
		}(idx)
	}
	if w.eventJob != nil {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.eventJob(len(w.jobs))
		}()
	}

	w.wg.Wait()
}
func (w *Worker[T]) WithCustomErrHandler(h func(error)) {
	w.errCh = make(chan error, 1)
	w.errHandler = h
}

func (w *Worker[T]) WithEvent(h func(T)) {
	if w.errHandler == nil {
		w.WithErrHandler()
	}

	w.ch = make(chan T, 1)
	w.eventJob = func(limit int) {
		defer func() {
			if r := recover(); r != nil {
				if w.errCh == nil {
					return
				}
				w.errCh <- errors.New(string(debug.Stack()))
			}
		}()
		for ; limit > 0; limit -= 1 {
			select {
			case r := <-w.ch:
				h(r)
			case err := <-w.errCh:
				w.errHandler(err)
			}
		}
	}
}

func (w *Worker[T]) WithErrHandler() {
	w.WithCustomErrHandler(func(err error) {
		w.errs = append(w.errs, err)
	})
}
func (w *Worker[T]) Error() error {
	return ErrsToErr(w.errs)
}

func ErrsToErr(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	p := GetByteBuffer()
	defer ClearAndPutByteBuffer(p)
	for idx := range errs {
		if idx > 0 {
			p.WriteByte('\n')
		}
		p.WriteString(errs[idx].Error())
	}
	return errors.New(p.String())
}
