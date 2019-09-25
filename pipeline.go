package pipeline

import (
	"fmt"
	"sync"
)

type Pipeline []Stage
type Stage interface {
	Run(inCh <-chan interface{}) (outCh <-chan interface{})
}
type ProducerFnc func(inCh <-chan interface{}) (outCh <-chan interface{})
type ConsumerFnc func(inCh <-chan interface{}) (outCh <-chan interface{})

func (fnc ProducerFnc) Run(inCh <-chan interface{}) (outCh <-chan interface{}) { return fnc(inCh) }
func (fnc ConsumerFnc) Run(inCh <-chan interface{}) (outCh <-chan interface{}) { return fnc(inCh) }

var (
	ErrEmptyPipeline     = fmt.Errorf("pipepile cannot be empty")
	ErrNilStage          = fmt.Errorf("stage cannot be nil")
	ErrProducerOnlyFirst = fmt.Errorf("producer can only be used as first stage")
)

func New(stages ...Stage) (Pipeline, error) {
	for idx, stage := range stages {
		if stage == nil {
			return nil, ErrNilStage
		}

		if idx > 0 {
			switch stage.(type) {
			case ProducerFnc:
				return nil, ErrProducerOnlyFirst
			}
		}
	}
	return stages, nil
}

func (p Pipeline) Run(inCh <-chan interface{}) (outCh <-chan interface{}) {
	ch := inCh
	for _, stage := range p {
		ch = stage.Run(ch)
	}
	return ch
}

func Producer(n int, fnc func(in <-chan interface{}) (out <-chan interface{})) Stage {
	return ProducerFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if fnc == nil || n <= 0 {
			return inCh
		}

		outCh := make(chan interface{}, n*5)
		wg := &sync.WaitGroup{}

		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				for out := range fnc(inCh) {
					outCh <- out
				}
			}()
		}
		go func() { wg.Wait(); close(outCh) }()
		return outCh
	})
}

func Consumer(n int, fnc func(obj interface{}) interface{}) Stage {
	return ConsumerFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if fnc == nil || n <= 0 {
			return inCh
		}

		outCh := make(chan interface{}, n*5)
		wg := &sync.WaitGroup{}

		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				for in := range inCh {
					outCh <- fnc(in)
				}
			}()
		}
		go func() { wg.Wait(); close(outCh) }()
		return outCh
	})
}
