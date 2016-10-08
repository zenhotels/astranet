package route

import (
	"sync"
	"sync/atomic"

	"github.com/zenhotels/dumbmap-2d/util"
)

type SecondaryLayer struct {
	lock   *sync.RWMutex
	ver    uint64
	offset uint64
	synced uint64 // id of the previously synced layer
	store  map[RouteInfo]*FinalizerList
}

func NewSecondaryLayer() *SecondaryLayer {
	return &SecondaryLayer{
		lock:   new(sync.RWMutex),
		offset: uint64(util.RevOffset()),
		store:  make(map[RouteInfo]*FinalizerList, 10),
	}
}

func (l *SecondaryLayer) Put(k RouteInfo, finalizers ...func()) (added int) {
	l.lock.Lock()
	l.verUp()
	if len(finalizers) == 0 {
		l.store[k] = nil
		l.lock.Unlock()
		return
	}

	list, ok := l.store[k]
	if !ok || list == nil {
		list = &FinalizerList{}
		l.store[k] = list
	}
	for i := range finalizers {
		if list.AddFinalizer(finalizers[i]) {
			added++
		}
	}
	l.lock.Unlock()
	return
}

func (l *SecondaryLayer) verUp() uint64 {
	return atomic.AddUint64(&l.ver, 1)
}

func (l *SecondaryLayer) Rev() uint64 {
	return atomic.LoadUint64(&l.ver) + l.offset
}

func (prev *SecondaryLayer) Sync(next *SecondaryLayer, onAdd, onDel func(key RouteInfo)) {
	nextRev := next.Rev()
	if prevRev := atomic.LoadUint64(&prev.synced); prevRev == nextRev {
		return
	}
	prev.lock.Lock()
	defer prev.lock.Unlock()
	next.lock.Lock()
	defer next.lock.Unlock()
	prev.synced = nextRev

	for key := range prev.store {
		if _, ok := next.store[key]; !ok {
			// deleted in the next revision
			if list := prev.store[key]; list != nil {
				list.Finalize()
			}
			prev.verUp()
			delete(prev.store, key)
			if onDel != nil {
				onDel(key)
			}
		}
	}
	for key := range next.store {
		if _, ok := prev.store[key]; !ok {
			// added in the next version
			prev.verUp()
			prev.store[key] = nil
			if onAdd != nil {
				onAdd(key)
			}
		}
	}
}

func (l *SecondaryLayer) ForEach(fn func(key RouteInfo, val *FinalizerList) bool) {
	l.lock.Lock()
	for key, val := range l.store {
		if stop := fn(key, val); stop {
			break
		}
	}
	l.lock.Unlock()
}

func (l *SecondaryLayer) Delete(key RouteInfo) (ok bool) {
	l.lock.Lock()
	var finalizers *FinalizerList
	finalizers, ok = l.store[key]
	if ok {
		l.verUp()
		delete(l.store, key)
	}
	l.lock.Unlock()
	if finalizers != nil {
		finalizers.Finalize()
	}
	return
}

func (l *SecondaryLayer) Finalize() {
	l.lock.RLock()
	for _, fns := range l.store {
		fns.Finalize()
	}
	l.lock.RUnlock()
}

func (l *SecondaryLayer) Len() int {
	l.lock.Lock()
	count := len(l.store)
	l.lock.Unlock()
	return count
}
