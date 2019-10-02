package pipeline

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFlushChan(t *testing.T) {
	ch := make(chan interface{})
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		flushChan(ch)
	}()

	failIfTimeout(t, 100*time.Millisecond, func() {
		// if the channel is blocked, this test failed (flushchan doesn't flush the chan)
		for i := 0; i < 1000; i++ {
			ch <- i
		}
	})

	close(ch)
	failIfTimeout(t, 100*time.Millisecond, func() {
		// if the Wait is blocked, this test failed (flushchan doesn't stop when the channel is closed)
		wg.Wait()
	})
}

func TestMultiplexChan(t *testing.T) {
	in := make(chan interface{})
	outA, outB := make(chan interface{}), make(chan interface{})
	go multiplexChan(in, outA, outB)
	defer close(in)

	failIfTimeout(t, 100*time.Millisecond, func() {
		in <- 10
		assert.Equal(t, 10, <-outA)
		assert.Equal(t, 10, <-outB)
	})
}
func TestMultiplexChan_Lock(t *testing.T) {
	in := make(chan interface{})
	outA, outB := make(chan interface{}), make(chan interface{})
	go multiplexChan(in, outA, outB)
	defer close(in)

	failIfTimeout(t, 100*time.Millisecond, func() {
		in <- 10
		<-outA
	})

	select {
	case in <- 10:
		t.Errorf("input channel must be blocked because outB is full")
		t.FailNow()
	case <-time.After(time.Millisecond):
	}

	failIfTimeout(t, time.Millisecond, func() {
		// clean all channels
		<-outB
	})
	select {
	case in <- 10:
	case <-time.After(time.Millisecond):
		t.Errorf("input channel must not be blocked")
		t.FailNow()
	}
}
func TestMultiplexChan_Close(t *testing.T) {
	in := make(chan interface{})
	outA, outB := make(chan interface{}), make(chan interface{})
	go multiplexChan(in, outA, outB)

	close(in)
	failIfTimeout(t, 100*time.Millisecond, func() {
		_, open := <-outA
		assert.False(t, open)
		_, open = <-outB
		assert.False(t, open)
	})
}
func TestMultiplexChanNoLock(t *testing.T) {
	in := make(chan interface{})
	// buffered chan is used here to allow to store value from input channel (because there is no lock,
	// value is dropped if it can be read immediately)
	outA, outB := make(chan interface{}, 1), make(chan interface{}, 1)
	go multiplexChanNoLock(in, outA, outB)
	defer close(in)

	failIfTimeout(t, 100*time.Millisecond, func() {
		in <- 10
		assert.Equal(t, 10, <-outA)
		assert.Equal(t, 10, <-outB)
	})
}
func TestMultiplexChanNoLock_NoLock(t *testing.T) {
	in := make(chan interface{})
	outA, outB := make(chan interface{}), make(chan interface{})
	go multiplexChanNoLock(in, outA, outB)
	defer close(in)

	// we can send several times the value, they are always dropped (because nobody reads the output channels)
	failIfTimeout(t, time.Millisecond, func() {
		in <- 10
		in <- 10
		in <- 10
		in <- 10
		in <- 10
	})
}
func TestMultiplexChanNoLock_Close(t *testing.T) {
	in := make(chan interface{})
	outA, outB := make(chan interface{}), make(chan interface{})
	go multiplexChanNoLock(in, outA, outB)

	close(in)
	failIfTimeout(t, 100*time.Millisecond, func() {
		_, open := <-outA
		assert.False(t, open)
		_, open = <-outB
		assert.False(t, open)
	})
}

func failIfTimeout(t *testing.T, timeout time.Duration, fnc func()) {
	done := make(chan interface{})
	go func() {
		defer close(done)
		fnc()
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Errorf("failIfTimeout: %s timed out (> %s)", t.Name(), timeout)
		t.FailNow()
	}
}
