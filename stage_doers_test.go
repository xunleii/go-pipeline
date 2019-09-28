package pipeline_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xunleii/go-pipeline"
)

func TestConsumer(t *testing.T) {
	c := pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 })

	in := make(chan interface{})
	out := c.Run(in)

	rnd := rand.Intn(1e6)
	in <- rnd
	close(in)

	assert.Equal(t, rnd*2, <-out)
	_, open := <-out
	assert.False(t, open)
}

func TestConsumer_NilChan(t *testing.T) {
	c := pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 })

	out := c.Run(nil)
	assert.Nil(t, out)
}

func TestConsumer_NilFunc(t *testing.T) {
	c := pipeline.C(nil)

	in := make(chan interface{})
	out := c.Run(in)

	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestProducer(t *testing.T) {
	p := pipeline.P(func(in <-chan interface{}) <-chan interface{} {
		out := make(chan interface{})
		go func() {
			for i := 0; i < 10; i++ {
				out <- i
			}
			close(out)
		}()
		return out
	})

	out := p.Run(nil)

	for i := 0; i < 10; i++ {
		assert.Equal(t, i, <-out)
	}
	_, open := <-out
	assert.False(t, open)
}

func TestProducer_NilFunc(t *testing.T) {
	p := pipeline.P(nil)

	in := make(chan interface{})
	out := p.Run(in)

	assert.Equal(t, (<-chan interface{})(in), out)
}
