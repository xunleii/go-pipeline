package pipeline_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xunleii/go-pipeline"
)

func TestConsumer(t *testing.T) {
	p, _ := pipeline.New(
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 5 }),
	)

	in := make(chan interface{})
	out := p.Run(in)

	in <- 1
	close(in)

	assert.Equal(t, 5, <-out)
	_, open := <-out
	assert.False(t, open)
}

func TestConsumer_NilChan(t *testing.T) {
	p, _ := pipeline.New(
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
	)

	out := p.Run(nil)
	assert.Nil(t, out)
}

func TestConsumer_NilFunc(t *testing.T) {
	p, _ := pipeline.New(
		pipeline.C(nil),
	)

	in := make(chan interface{})
	out := p.Run(in)

	var exIn <-chan interface{}
	exIn = in
	assert.Equal(t, exIn, out)
}

func TestProducer(t *testing.T) {
	p, _ := pipeline.New(
		pipeline.P(func(in <-chan interface{}) <-chan interface{} {
			out := make(chan interface{})
			go func() {
				for i := 0; i < 10; i++ {
					out <- i
				}
				close(out)
			}()
			return out
		}),
	)

	out := p.Run(nil)

	for i := 0; i < 10; i++ {
		assert.Equal(t, i, <-out)
	}
	_, open := <-out
	assert.False(t, open)
}

func TestProducer_NilFunc(t *testing.T) {
	p, _ := pipeline.New(
		pipeline.P(nil),
	)

	in := make(chan interface{})
	out := p.Run(in)

	var exIn <-chan interface{}
	exIn = in
	assert.Equal(t, exIn, out)
}
