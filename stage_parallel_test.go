package pipeline_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xunleii/go-pipeline"
)

func TestParallelize(t *testing.T) {
	p := pipeline.Parallelize(10, pipeline.C(func(obj interface{}) interface{} { time.Sleep(250 * time.Millisecond); return obj }))

	in := make(chan interface{})
	start := time.Now()
	out := p.Run(in)

	for i := 0; i < 10; i++ {
		in <- 1
	}

	close(in)
	for range out {
	}

	assert.WithinDuration(t, time.Now(), start.Add(250*time.Millisecond), 100*time.Millisecond)
}

func TestParallelize_Zero(t *testing.T) {
	p := pipeline.Parallelize(0, pipeline.C(func(obj interface{}) interface{} { time.Sleep(250 * time.Millisecond); return obj }))

	in := make(chan interface{})
	out := p.Run(in)
	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestParallelize_NilStage(t *testing.T) {
	p := pipeline.Parallelize(10, nil)

	in := make(chan interface{})
	out := p.Run(in)
	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestParallelize_NilChan(t *testing.T) {
	p := pipeline.Parallelize(10, pipeline.C(func(obj interface{}) interface{} { time.Sleep(250 * time.Millisecond); return obj }))

	out := p.Run(nil)
	assert.Nil(t, out)
}

func TestFork(t *testing.T) {
	f := pipeline.Fork(
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
	)

	in := make(chan interface{})
	out := f.Run(in)

	in <- 5
	close(in)

	assert.Equal(t, 10, <-out)
	assert.Equal(t, 10, <-out)
	_, open := <-out
	assert.False(t, open)
}

func TestFork_NoStage(t *testing.T) {
	f := pipeline.Fork()

	in := make(chan interface{})
	out := f.Run(in)
	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestFork_NilStage(t *testing.T) {
	f := pipeline.Fork(
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		nil,
	)

	in := make(chan interface{})
	out := f.Run(in)
	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestFork_NilChan(t *testing.T) {
	f := pipeline.Fork(
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
	)

	out := f.Run(nil)
	assert.Nil(t, out)
}

func TestFork_Blocked(t *testing.T) {
	lock := make(chan interface{})
	f := pipeline.Fork(
		pipeline.C(func(obj interface{}) interface{} { <-lock; return obj.(int) * 2 }),
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
	)

	in := make(chan interface{})
	out := f.Run(in)

	go func() {
		for range out {
		}
	}()

	in <- 5 // lock the first forked stage
	in <- 5 // lock the fork stage
	select {
	case in <- 5:
		t.Errorf("input channel must be blocked")
	case <-time.After(time.Millisecond):
	}

	close(lock) // unlock first forked stage
	select {
	case in <- 5:
	case <-time.After(time.Millisecond):
		t.Errorf("input channel must not be blocked")
	}
}

func TestMirror(t *testing.T) {
	f := pipeline.Mirror(
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
	)

	in := make(chan interface{})
	out := f.Run(in)

	in <- 5
	close(in)

	assert.Equal(t, 10, <-out)
	assert.Equal(t, 10, <-out)
	_, open := <-out
	assert.False(t, open)
}

func TestMirror_NoStage(t *testing.T) {
	f := pipeline.Mirror()

	in := make(chan interface{})
	out := f.Run(in)
	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestMirror_NilStage(t *testing.T) {
	f := pipeline.Mirror(
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		nil,
	)

	in := make(chan interface{})
	out := f.Run(in)
	assert.Equal(t, (<-chan interface{})(in), out)
}

func TestMirror_NilChan(t *testing.T) {
	f := pipeline.Mirror(
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
	)

	out := f.Run(nil)
	assert.Nil(t, out)
}

func TestMirror_NotBlocked(t *testing.T) {
	lock := make(chan interface{})
	f := pipeline.Mirror(
		pipeline.C(func(obj interface{}) interface{} { <-lock; return obj.(int) * 2 }),
		pipeline.C(func(obj interface{}) interface{} { return obj.(int) * 2 }),
	)

	in := make(chan interface{})
	out := f.Run(in)

	go func() {
		for range out {
		}
	}()

	in <- 5 // lock the first forked stage (not here)
	in <- 5 // lock the fork stage (not here)
	select {
	case in <- 5:
	case <-time.After(time.Millisecond):
		t.Errorf("input channel must not be blocked")
	}

	close(lock) // unlock first forked stage
	select {
	case in <- 5:
	case <-time.After(time.Millisecond):
		t.Errorf("input channel must not be blocked")
	}
}

func TestMirror_AllBlocked(t *testing.T) {
	lock := make(chan interface{})
	f := pipeline.Mirror(
		pipeline.C(func(obj interface{}) interface{} { <-lock; return obj.(int) * 2 }),
		pipeline.C(func(obj interface{}) interface{} { <-lock; return obj.(int) * 2 }),
	)

	in := make(chan interface{})
	out := f.Run(in)

	go func() {
		for range out {
		}
	}()

	in <- 5 // lock the first forked stage
	in <- 5 // lock the fork stage
	select {
	case in <- 5:
		t.Errorf("input channel must be blocked")
	case <-time.After(time.Millisecond):
	}

	close(lock) // unlock first forked stage
	select {
	case in <- 5:
	case <-time.After(time.Millisecond):
		t.Errorf("input channel must not be blocked")
	}
}
