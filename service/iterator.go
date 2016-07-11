package service

import "sync/atomic"

type Iterator struct {
	*Registry
	last uint64
}

func (self Iterator) Next() Iterator {
	var last = atomic.LoadUint64(&self.rRev)
	self.rLock.RLock()
	var swapped = atomic.CompareAndSwapUint64(&self.rRev, self.last, self.last)
	var closed = self.closed
	if swapped && !closed {
		self.rCond.Wait()
	}
	self.rLock.RUnlock()
	return Iterator{self.Registry, last}
}
