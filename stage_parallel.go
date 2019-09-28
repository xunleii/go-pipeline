package pipeline

import "sync"

// Parallelize runs n times the given stage and merge theirs outputs in one channel.
func Parallelize(n int, stage Stage) Stage {
	return StageFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if stage == nil || inCh == nil {
			return inCh
		}

		wg := &sync.WaitGroup{}

		outCh := make(chan interface{}, cap(inCh)*n) // We allow each stage to have a full size channel
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				for out := range stage.Run(inCh) {
					outCh <- out
				}
			}()
		}
		go func() { wg.Wait(); close(outCh) }() // Close outCh only when all goroutine are stopped
		return outCh
	})
}

// Fork runs all given stage in parallel by duplicating all value received to all stages. When one
// of the given stages is blocked, all of this stage is blocked. Use LazyFork to block only if all
// given stages are blocked.
func Fork(stages ...Stage) Stage {
	return StageFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if len(stages) == 0 || hasNilStage(stages) {
			return inCh
		}

		outCh := make(chan interface{}, cap(inCh)*len(stages)) // We allow each stage to have a full size channel
		dupInChs := make([]chan interface{}, len(stages))
		wg := &sync.WaitGroup{}
		wg.Add(len(stages))
		for i, stage := range stages {
			dupInChs[i] = make(chan interface{}, cap(inCh))
			go func() {
				defer wg.Done()
				for out := range stage.Run(dupInChs[i]) {
					outCh <- out
				}
			}()
		}

		go func() {
			for in := range inCh {
				for _, dupInCh := range dupInChs {
					dupInCh <- in
				}
			}
			wg.Wait()
			close(outCh)
		}()

		return outCh
	})
}

// LazyFork runs all given stage in parallel by duplicating all value received to all stages. When one
// of the given stages is blocked, the next value is dropped. Use Fork to block if one of the given stages
// is blocked.
func LazyFork(stages ...Stage) Stage {
	return StageFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if len(stages) == 0 || hasNilStage(stages) {
			return inCh
		}

		outCh := make(chan interface{}, cap(inCh)*len(stages)) // We allow each stage to have a full size channel
		dupInChs := make([]chan interface{}, len(stages))
		wg := &sync.WaitGroup{}
		wg.Add(len(stages))
		for i, stage := range stages {
			dupInChs[i] = make(chan interface{}, cap(inCh))
			go func() {
				defer wg.Done()
				for out := range stage.Run(dupInChs[i]) {
					outCh <- out
				}
			}()
		}

		go func() {
			for in := range inCh {
				blocked := 0
				for _, dupInCh := range dupInChs {
					// We drop the value if:
					//	- len(dupInCh) == cap(dupInCh): the input channel is full
					//	- blocked != len(dupInChs) - 2: all input channel are blocked ... so we need to block the pipeline
					if len(dupInCh) < cap(dupInCh) || blocked == len(dupInChs)-2 {
						dupInCh <- in
					} else {
						blocked++
					}
				}
			}
			wg.Wait()
			close(outCh)
		}()

		return outCh
	})
}
