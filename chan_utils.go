package pipeline

func flushChan(inCh <-chan interface{}) {
	for range inCh {
	}
}

func multiplexChan(inCh <-chan interface{}, outChs ...chan interface{}) {
	for in := range inCh {
		for _, chOut := range outChs {
			chOut <- in
		}
	}

	for _, chOut := range outChs {
		close(chOut)
	}
}

func multiplexChanNoLock(inCh <-chan interface{}, outChs ...chan interface{}) {
	for in := range inCh {
		for _, chOut := range outChs {
			select {
			case chOut <- in:
			default:
			}
		}
	}

	for _, chOut := range outChs {
		close(chOut)
	}
}
