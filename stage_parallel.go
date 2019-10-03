package pipeline

import (
	"sync"
)

// Parallelize runs n times the given stage and merge theirs outputs in one channel.
func Parallelize(n int, stage Stage) Stage {
	return StageFnc(func(in <-chan interface{}) <-chan interface{} {
		if n == 0 || stage == nil || in == nil {
			return in
		}

		out := make(chan interface{}, cap(in)*n) // We allow each stage to have a full size channel

		wg := &sync.WaitGroup{}
		wg.Add(n)
		for i := 0; i < n; i++ {
			go innerStage(stage, wg, in, out)
		}
		go func() { wg.Wait(); close(out) }() // Close out only when all goroutine are stopped
		return out
	})
}

// Fork runs all given stage in parallel by duplicating all value received to all stages. When one
// of the given stages is blocked, this stage is blocked. Use Mirror to block only if the
// first stage are blocked.
func Fork(stages ...Stage) Stage {
	return StageFnc(func(in <-chan interface{}) <-chan interface{} {
		if len(stages) == 0 || hasNilStage(stages) || in == nil {
			return in
		}

		out := make(chan interface{}, cap(in)*len(stages)) // We allow each stage to have a full size channel
		chs := make([]chan interface{}, len(stages))

		wg := &sync.WaitGroup{}
		wg.Add(len(stages))
		for i, stage := range stages {
			chs[i] = make(chan interface{}, cap(in))
			go innerStage(stage, wg, chs[i], out)
		}
		go multiplexChan(in, chs...)
		go func() { wg.Wait(); close(out) }()
		return out
	})
}

// Mirror runs all given stage in parallel by duplicating all value received to all stages. When the main stage is
// blocked, this stage is blocked. When one of the given mirrors is blocked, the next value is dropped. Use Fork to
// block the stage when one of the given stages is blocked.
func Mirror(main Stage, mirrors ...Stage) Stage {
	return StageFnc(func(in <-chan interface{}) <-chan interface{} {
		if main == nil || len(mirrors) == 0 || hasNilStage(mirrors) || in == nil {
			return in
		}

		mainCh := make(chan interface{}, cap(in))
		mirrorsCh := make(chan interface{}, cap(in)*(len(mirrors)+1)) // Input channel for multiplexed mirrors
		out := make(chan interface{}, cap(in)*(len(mirrors)+1))       // We allow each stage to have a full size channel
		chs := make([]chan interface{}, len(mirrors))

		wg := &sync.WaitGroup{}
		wg.Add(len(mirrors) + 1)
		go innerStage(main, wg, mainCh, out)
		for i, stage := range mirrors {
			chs[i] = make(chan interface{}, cap(in))
			go innerStage(stage, wg, chs[i], out)
		}

		go func() {
			for in := range in {
				mainCh <- in
				mirrorsCh <- in
			}

			close(mainCh)
			close(mirrorsCh)
		}()

		go multiplexChanNoLock(mirrorsCh, chs...)
		go func() { wg.Wait(); close(out) }()
		return out
	})
}

func innerStage(stage Stage, wg *sync.WaitGroup, in <-chan interface{}, out chan<- interface{}) {
	defer wg.Done()
	for ch := range stage.Run(in) {
		out <- ch
	}

	// avoid to block inCh if stage.Run close its output channel unexpectedly but allows to
	// close global output channel
	go flushChan(in)
}
