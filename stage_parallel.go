package pipeline

import (
	"fmt"
	"sync"
	"time"
)

// Parallelize runs n times the given stage and merge theirs outputs in one channel.
func Parallelize(n int, stage Stage) Stage {
	return StageFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if n == 0 || stage == nil || inCh == nil {
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
// of the given stages is blocked, all of this stage is blocked. Use Mirror to block only if all
// given stages are blocked.
func Fork(stages ...Stage) Stage {
	return StageFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if len(stages) == 0 || hasNilStage(stages) || inCh == nil {
			return inCh
		}

		outCh := make(chan interface{}, cap(inCh)*len(stages)) // We allow each stage to have a full size channel
		dupInChs := make([]chan interface{}, len(stages))
		wg := &sync.WaitGroup{}
		wg.Add(len(stages))
		for i, stage := range stages {
			dupInChs[i] = make(chan interface{}, cap(inCh))
			go func(stage Stage, inCh <-chan interface{}) {
				defer wg.Done()
				for out := range stage.Run(inCh) {
					outCh <- out
				}
			}(stage, dupInChs[i])
		}

		go func() {
			for in := range inCh {
				for _, dupInCh := range dupInChs {
					dupInCh <- in
				}
			}

			for _, dupInCh := range dupInChs {
				close(dupInCh)
			}
			wg.Wait()
			close(outCh)
		}()

		return outCh
	})
}

// Mirror runs all given stage in parallel by duplicating all value received to all stages. When one
// of the given stages is blocked, the next value is dropped. Use Fork to block the stage if at least one
// of the given stages was blocked.
func Mirror(stages ...Stage) Stage {
	return StageFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if len(stages) == 0 || hasNilStage(stages) || inCh == nil {
			return inCh
		}

		outCh := make(chan interface{}, cap(inCh)*len(stages)) // We allow each stage to have a full size channel
		dupInChs := make([]chan interface{}, len(stages))
		wg := &sync.WaitGroup{}
		wg.Add(len(stages))
		for i, stage := range stages {
			dupInChs[i] = make(chan interface{}, cap(inCh))
			go func(stage Stage, inCh <-chan interface{}) {
				defer wg.Done()
				for out := range stage.Run(inCh) {
					outCh <- out
				}
			}(stage, dupInChs[i])
		}

		go func() {
			for in := range inCh {
				blocked := 0
				for _, dupInCh := range dupInChs {
					// if all forked stage are blocked (without the last), we try to send the value directly in
					// the channel, without check.
					if blocked == len(dupInChs)-1 {
						fmt.Println("BLOCKED")
						dupInCh <- in
					} else {
						// We try to send the value in the channel. If the channel is blocking, we drop the value.
						select {
						case dupInCh <- in:
						case <-time.After(time.Millisecond):
							fmt.Println("BLOCKED")
							blocked++
						}
					}
				}
			}
			wg.Wait()
			close(outCh)
		}()

		return outCh
	})
}
