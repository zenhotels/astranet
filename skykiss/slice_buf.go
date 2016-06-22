package skykiss

import "sync"

type SliceBufAgent struct {
	Bytes []byte
}

type SliceBuf struct {
	pool *sync.Pool
}

func SliceBufNew(pSize int) *SliceBuf {
	return &SliceBuf{
		pool: &sync.Pool{
			New: func() interface{} {
				return &SliceBufAgent{
					Bytes: make([]byte, pSize),
				}
			},
		},
	}
}

func (self *SliceBuf) Acquire() *SliceBufAgent {
	return self.pool.Get().(*SliceBufAgent)
}

func (self *SliceBuf) Release(b *SliceBufAgent) {
	self.pool.Put(b)
}
