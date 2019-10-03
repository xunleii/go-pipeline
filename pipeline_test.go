package pipeline_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xunleii/go-pipeline"
)

func TestPipeline_Run(t *testing.T) {
	p := pipeline.Pipeline{
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 5 }),
	}

	in := make(chan interface{})
	out := p.Run(in)

	rnd := rand.Intn(1e6)
	in <- rnd
	close(in)

	assert.Equal(t, rnd*10, <-out)
	_, open := <-out
	assert.False(t, open)
}

func TestPipeline_Empty(t *testing.T) {
	p := pipeline.Pipeline{}

	in := make(chan interface{})
	out := p.Run(in)

	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestPipeline_NilStage(t *testing.T) {
	p := pipeline.Pipeline{
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		nil,
	}

	in := make(chan interface{})
	out := p.Run(in)

	assert.Equal(t, (<-chan interface{})(in), out)
}
