package pipeline_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xunleii/pipeline"
)

func TestNew_Err(t *testing.T) {
	tests := []struct {
		name   string
		stages []pipeline.Stage
		err    error
	}{
		{"EmptyPipeline", []pipeline.Stage{}, pipeline.ErrEmptyPipeline},
		{"NilStage", []pipeline.Stage{nil}, pipeline.ErrEmptyPipeline},
		{"ProducerNotFirst", []pipeline.Stage{pipeline.Consumer(0, nil), pipeline.Producer(0, nil)}, pipeline.ErrEmptyPipeline},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pipeline.New(tt.stages...)
			assert.EqualError(t, err, tt.err.Error())
		})
	}
}

func TestNew(t *testing.T) {
	pipeline, _ := pipeline.New(
		pipeline.Producer(2, func(<-chan interface{}) <-chan interface{} {
			out := make(chan interface{})

			go func() {
				defer close(out)
				for i := 1; i <= 10; i++ {
					out <- i
				}
			}()
			return out
		}),
		pipeline.Consumer(6, func(obj interface{}) interface{} { return obj.(int) * 5 }),
		pipeline.Consumer(3, func(obj interface{}) interface{} { return obj.(int) + 2 }),
	)

	var numbers []int
	for num := range pipeline.Run(nil) {
		numbers = append(numbers, num.(int))
		assert.True(t, num.(int) >= 7)
	}
	assert.Len(t, numbers, 20)
}

func TestNew_ManualClose(t *testing.T) {
	pipeline, _ := pipeline.New(
		pipeline.Producer(2, func(in <-chan interface{}) <-chan interface{} {
			out := make(chan interface{})

			go func() {
				defer close(out)

				tick := time.Tick(50 * time.Millisecond)
				for {
					select {
					case <-in:
						return
					case <-tick:
						out <- 1
					}
				}
			}()
			return out
		}),
		pipeline.Consumer(6, func(obj interface{}) interface{} { return obj.(int) * 5 }),
		pipeline.Consumer(3, func(obj interface{}) interface{} { return obj.(int) + 2 }),
	)

	sig := make(chan interface{})
	out := pipeline.Run(sig)

	sw := time.Now()
	time.Sleep(500 * time.Millisecond)
	close(sig)
	for range out {
	}
	assert.WithinDuration(t, sw.Add(500*time.Millisecond), time.Now(), 100*time.Millisecond)
}
