package astranet

import (
	"sync"
	"time"
)

type StreamWFlow struct {
	id     uint64
	buf    []byte
	rDone  int
	ackErr error
	rErr   error
	done   sync.Cond
	doneL  sync.Mutex
	closed bool

	deadLine *time.Time
}

func (self *StreamWFlow) Deadline() time.Time {
	return *self.deadLine
}

func (self *StreamWFlow) Stream() uint64 {
	return self.id
}

func (self *StreamWFlow) Ack(bSize int) ([]byte, error) {
	if bSize > len(self.buf) {
		bSize = len(self.buf)
	}
	return self.buf[0:bSize], self.ackErr
}

func (self *StreamWFlow) Fin(n int, err error) int {
	self.rDone += n
	self.buf = self.buf[n:]
	self.rErr = err
	if len(self.buf) == 0 || err != nil {
		self.closed = true
		self.done.Broadcast()
		return 0
	}
	return len(self.buf)
}

func (self *StreamWFlow) Join() {
	self.done.L.Lock()
	for {
		if self.closed {
			break
		}
		self.done.Wait()
	}
	self.done.L.Unlock()
}