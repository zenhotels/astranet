package astranet

import (
	"sync"
	"time"
)

type StreamRFlow struct {
	id     uint64
	buf    []byte
	rDone  int
	rErr   error
	done   sync.Cond
	doneL  sync.Mutex
	closed bool

	deadLine *time.Time
}

func (self *StreamRFlow) Deadline() time.Time {
	return *self.deadLine
}

func (self *StreamRFlow) Stream() uint64 {
	return self.id
}

func (self *StreamRFlow) Syn() []byte {
	return self.buf
}

func (self *StreamRFlow) Fin(n int, err error) {
	self.rDone, self.rErr = n, err
	self.closed = true
	self.done.Broadcast()
}

func (self *StreamRFlow) Join() {
	self.done.L.Lock()
	for {
		if self.closed {
			break
		}
		self.done.Wait()
	}
	self.done.L.Unlock()
}
