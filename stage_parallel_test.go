package pipeline_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xunleii/go-pipeline"
)

func TestScale(t *testing.T) {
	p, _ := pipeline.New(
		pipeline.Parallelize(10, pipeline.C(func(obj interface{}) interface{} { time.Sleep(250 * time.Millisecond); return obj })),
	)

	in := make(chan interface{})
	start := time.Now()
	out := p.Run(in)

	for i := 0; i < 10; i++ {
		in <- 1
	}
	close(in)

	for range out {
	}

	_, open := <-out
	assert.False(t, open)
	assert.WithinDuration(t, time.Now(), start.Add(250*time.Millisecond), 100*time.Millisecond)
}
