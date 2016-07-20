package btree2d

import (
	"sync"
	"sync/atomic"
)

const MaxFinalizers = 16

type FinalizerList struct {
	offset      int64
	list        []func()
	onceNewList sync.Once
}

func (f *FinalizerList) Len() int {
	offset := int(atomic.LoadInt64(&f.offset))
	if offset >= 100 {
		return 0
	} else if offset > MaxFinalizers {
		return MaxFinalizers
	}
	return offset
}

func (f *FinalizerList) Finalize() {
	offset := atomic.SwapInt64(&f.offset, 100)
	if offset == 0 || offset >= 100 {
		return
	} else if offset > MaxFinalizers {
		offset = MaxFinalizers
	}
	_ = f.list[MaxFinalizers-1]
	for i := int64(0); i < offset; i++ {
		go f.list[i]()
	}
	f.list = nil // protected by swapInt64
}

func (f *FinalizerList) AddFinalizer(fn func()) bool {
	f.onceNewList.Do(func() {
		f.list = make([]func(), MaxFinalizers)
	})
	offset := atomic.AddInt64(&f.offset, 1)
	if offset > MaxFinalizers {
		atomic.AddInt64(&f.offset, -1)
		return false
	}
	f.list[offset-1] = fn
	return true
}
