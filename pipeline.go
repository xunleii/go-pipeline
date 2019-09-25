package pipeline

import (
	"fmt"
)

// Stage is a step executed in parallel on sequentially and composing a pipeline. This is the main
// component of this package.
// IMPORTANT: The stage must close the output channel when the input channel is closed (to propagate
// the close signal and stop the pipeline).
type Stage interface {
	Run(inCh <-chan interface{}) (outCh <-chan interface{})
}

// Pipeline is a sequence of stages executed concurrently.
type Pipeline []Stage

// BufferedChanSize is the size of each buffered channel. This can be change globally for all stages.
var BufferedChanSize = 32

// Pipelines errors
var (
	ErrEmptyPipeline = fmt.Errorf("pipeline cannot be empty")
	ErrNilStage      = fmt.Errorf("stage cannot be nil")
)

// New creates a new Pipeline from stage definitions.
func New(stages ...Stage) (Pipeline, error) {
	switch {
	case len(stages) == 0:
		return nil, ErrEmptyPipeline
	case hasNilStage(stages):
		return nil, ErrNilStage
	default:
		return stages, nil
	}
}

// Run start all stages. The pipeline can be stopped if the given channel is closed (or by the
// producer if a producer is used).
func (p Pipeline) Run(inCh <-chan interface{}) (outCh <-chan interface{}) {
	ch := inCh
	for _, stage := range p {
		ch = stage.Run(ch)
	}
	return ch
}

// StageFnc is a generic function that implements the stage interface.
type StageFnc func(inCh <-chan interface{}) (outCh <-chan interface{})

func (fnc StageFnc) Run(inCh <-chan interface{}) (outCh <-chan interface{}) { return fnc(inCh) }

func hasNilStage(stages []Stage) bool {
	for _, stage := range stages {
		if stage == nil {
			return true
		}
	}
	return false
}
