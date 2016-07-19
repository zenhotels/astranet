package btree2d

import (
	"sync"
	"sync/atomic"

	"github.com/zenhotels/btree-2d/common"
)

const MaxFinalizers = 16

func NewFinalizable(obj common.Comparable) common.FinalizableComparable {
	return &withFinalizers{
		obj: obj,
	}
}

func NonFinalizable(obj common.Comparable) common.FinalizableComparable {
	return &withoutFinalizers{
		obj: obj,
	}
}

type withoutFinalizers struct {
	obj common.Comparable
}

func (w *withoutFinalizers) Less(w2 common.Comparable) bool {
	return w.obj.Less(w2.(*withoutFinalizers).obj)
}

func (w *withoutFinalizers) Finalize() {

}

func (w *withoutFinalizers) Value() common.Comparable {
	return w.obj
}

func (w *withoutFinalizers) AddFinalizer(fn func()) bool {
	return false
}

type withFinalizers struct {
	obj common.Comparable

	offset      int64
	list        []func()
	onceNewList sync.Once
}

func (w *withFinalizers) Value() common.Comparable {
	return w.obj
}

func (w *withFinalizers) Less(w2 common.Comparable) bool {
	return w.obj.Less(w2.(*withFinalizers).obj)
}

func (w *withFinalizers) Finalize() {
	w.onceNewList.Do(func() {
		w.list = make([]func(), MaxFinalizers)
	})
	offset := atomic.SwapInt64(&w.offset, 100)
	if offset >= 100 {
		return
	} else if offset > MaxFinalizers {
		offset = MaxFinalizers
	}
	_ = w.list[MaxFinalizers-1]
	for i := int64(0); i < offset; i++ {
		go w.list[i]()
	}
	w.list = nil // protected by swapInt64
}

func (w *withFinalizers) AddFinalizer(fn func()) bool {
	w.onceNewList.Do(func() {
		w.list = make([]func(), MaxFinalizers)
	})
	offset := atomic.AddInt64(&w.offset, 1)
	if offset > MaxFinalizers {
		atomic.AddInt64(&w.offset, -1)
		return false
	}
	w.list[offset-1] = fn
	return true
}
