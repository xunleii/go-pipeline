package pipeline

// P is a short alias for Producer
func P(fnc func(in <-chan interface{}) (out <-chan interface{})) Stage { return Producer(fnc) }

// Producer creates value used by other stages in the pipeline.
func Producer(fnc func(in <-chan interface{}) <-chan interface{}) Stage {
	return StageFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if fnc == nil {
			return inCh
		}

		outCh := make(chan interface{}, BufferedChanSize)
		go func() {
			defer close(outCh)

			for out := range fnc(inCh) {
				outCh <- out
			}
		}()
		return outCh
	})
}

// C is a short alias for Consumer
func C(fnc func(obj interface{}) interface{}) Stage { return Consumer(fnc) }

// Consumer is the main 'worker'; it consume the given object and return another one.
func Consumer(fnc func(obj interface{}) interface{}) Stage {
	return StageFnc(func(inCh <-chan interface{}) <-chan interface{} {
		if fnc == nil || inCh == nil {
			return inCh
		}

		outCh := make(chan interface{}, BufferedChanSize)
		go func() {
			defer close(outCh)

			for in := range inCh {
				outCh <- fnc(in)
			}
		}()
		return outCh
	})
}
