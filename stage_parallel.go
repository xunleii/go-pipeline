package pipeline

import "sync"

// Scale runs n times the given stage and merge theirs outputs in one channel.
func Scale(n int, stage Stage) Stage {
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

// Parallelize runs all given stage in parallel by duplicating all value received to all stages. When one
// of the given stages is blocked, all of this stage is blocked. Use UnsafeParallelize to ignore to block
// only if all given stages are blocked.
// TODO: Create UnsafeParallelize
func Parallelize(stages ...Stage) Stage {
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
