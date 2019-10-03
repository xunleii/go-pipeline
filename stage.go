package pipeline

// BufferedChanSize is the size of each buffered channel. This can be change globally for all stages.
var BufferedChanSize = 32

// Stage is a step executed in parallel on sequentially and composing a pipeline. This is the main
// component of this package.
// IMPORTANT: The stage must close the output channel when the input channel is closed (to propagate
// the close signal and stop the pipeline).
type Stage interface {
	Run(inCh <-chan interface{}) (outCh <-chan interface{})
}

// StageFnc is a generic function that implements the stage interface.
type StageFnc func(inCh <-chan interface{}) (outCh <-chan interface{})

func (fnc StageFnc) Run(inCh <-chan interface{}) (outCh <-chan interface{}) { return fnc(inCh) }
