package pipeline

import "sync"

// Predicate is a function that returns true or false depending on the given input.
type Predicate func(interface{}) bool

// LRFilter (or Left/Right Filter) filters values depending on the given predicate. For each value
// of the input channel, if the predicate returns true, the value is sent to the left pipeline,
// otherwise, the value is sent to the right one.
func LRFilter(predicate Predicate, left Pipeline, right Pipeline) Stage {
	return StageFnc(func(in <-chan interface{}) <-chan interface{} {
		if predicate == nil || (len(left) == 0 && len(right) == 0) || (hasNilStage(left) && hasNilStage(right)) {
			return in
		}

		lchan := make(chan interface{}, (cap(in)/2)+1)
		rchan := make(chan interface{}, (cap(in)/2)+1)
		out := make(chan interface{}, cap(in))

		wg := &sync.WaitGroup{}
		wg.Add(2)
		go runPipeline(left, lchan, out, wg)
		go runPipeline(right, rchan, out, wg)

		go func() {
			defer close(lchan)
			defer close(rchan)

			for value := range in {
				if predicate(value) {
					lchan <- value
				} else {
					rchan <- value
				}
			}
		}()
		go func() { wg.Wait(); close(out) }() // Close out only when all goroutine are stopped
		return out
	})
}

func runPipeline(pipeline Pipeline, in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	values := pipeline.Run(in)
	for value := range values {
		out <- value
	}
}
