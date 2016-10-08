package service

import "sync"

const MaxFinalizers = 16

type FinalizerList struct {
	mux  sync.RWMutex
	list []func()
}

func (f *FinalizerList) Len() int {
	f.mux.Lock()
	l := len(f.list)
	f.mux.Unlock()
	return l
}

func (f *FinalizerList) Finalize() {
	f.mux.Lock()
	defer f.mux.Unlock()
	for i := range f.list {
		fn := f.list[i]
		go fn()
	}
	f.list = nil
}

func (f *FinalizerList) AddFinalizer(fn func()) (added bool) {
	f.mux.Lock()
	if len(f.list) < MaxFinalizers {
		added = true
		f.list = append(f.list, fn)
	}
	f.mux.Unlock()
	return
}
