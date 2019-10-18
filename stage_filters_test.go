package pipeline_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xunleii/go-pipeline"
)

func TestLRFilter_Run(t *testing.T) {
	f := pipeline.LRFilter(
		func(i interface{}) bool { return i.(int) < 0 },
		pipeline.Pipeline{
			pipeline.C(func(obj interface{}) interface{} { return obj }),
		},
		pipeline.Pipeline{
			pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		},
	)

	in := make(chan interface{})
	out := f.Run(in)

	rnd := rand.Intn(1e6) // Positive number: right pipeline (value * 2)
	in <- rnd
	assert.Equal(t, rnd*2, <-out)

	rnd = rand.Intn(1e6) * -1 // Negative number: left pipeline (value)
	in <- rnd
	assert.Equal(t, rnd, <-out)

	close(in)
	_, open := <-out
	assert.False(t, open)
}

func TestLRFilter_NilPredicate(t *testing.T) {
	f := pipeline.LRFilter(
		nil,
		pipeline.Pipeline{
			pipeline.C(func(obj interface{}) interface{} { return obj }),
		},
		pipeline.Pipeline{
			pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		},
	)

	in := make(chan interface{})
	out := f.Run(in)

	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestLRFilter_NoPipeline(t *testing.T) {
	f := pipeline.LRFilter(
		func(i interface{}) bool { return i.(int) < 0 },
		pipeline.Pipeline{},
		pipeline.Pipeline{},
	)

	in := make(chan interface{})
	out := f.Run(in)

	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestLRFilter_InvalidPipeline(t *testing.T) {
	f := pipeline.LRFilter(
		func(i interface{}) bool { return i.(int) < 0 },
		pipeline.Pipeline{
			pipeline.C(func(obj interface{}) interface{} { return obj }),
			nil,
		},
		pipeline.Pipeline{
			pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
			nil,
		},
	)

	in := make(chan interface{})
	out := f.Run(in)

	assert.Equal(t, (<-chan interface{})(in), out)
}
