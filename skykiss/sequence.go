package skykiss

import "sync/atomic"

type AutoIncSequence struct {
	lastSeq uint64
}

func (self *AutoIncSequence) Next() uint64 {
	return atomic.AddUint64(&self.lastSeq, 1)
}

func (self *AutoIncSequence) Last() uint64 {
	return self.lastSeq
}

func (self *AutoIncSequence) Reset(v uint64)  {
	atomic.StoreUint64(&self.lastSeq, v)
}
