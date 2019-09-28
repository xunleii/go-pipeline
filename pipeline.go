package pipeline

// Pipeline is a sequence of stages executed concurrently.
type Pipeline []Stage

// Run start all stages. The pipeline can be stopped if the given channel is closed (or by the
// producer if a producer is used).
func (p Pipeline) Run(inCh <-chan interface{}) (outCh <-chan interface{}) {
	if len(p) == 0 || hasNilStage(p) {
		return inCh
	}

	ch := inCh
	for _, stage := range p {
		ch = stage.Run(ch)
	}
	return ch
}

func hasNilStage(stages []Stage) bool {
	for _, stage := range stages {
		if stage == nil {
			return true
		}
	}
	return false
}
