package service

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/zenhotels/astranet/skykiss"
)

type Registry struct {
	sMap    BTree2D
	rLock   sync.RWMutex
	rCond   sync.Cond
	rRev    uint64
	closed  uint64
	initCtl sync.Once
}

func (self *Registry) init() {
	self.initCtl.Do(func() {
		self.sMap = BTreeNew()
		self.rCond.L = &self.rLock
	})
}

func (self *Registry) touch() {
	atomic.AddUint64(&self.rRev, 1)
	self.rCond.Broadcast()
}

func (self *Registry) Push(id string, srv ServiceInfo, action ...func()) {
	self.init()
	var closed = atomic.LoadUint64(&self.closed)
	if closed > 0 {
		return
	}
	self.sMap.Put(id, srv, action...)
	self.touch()
}

func (self *Registry) Pop(id string, srv ServiceInfo) {
	self.init()
	self.sMap.Delete(id, srv)
	self.touch()
}

func (self *Registry) DiscoverTimeout(r Selector, sname string, wait time.Duration) (srv ServiceInfo, found bool) {
	self.init()

	var started = time.Now()
	var stopAt = started.Add(wait)
	self.rLock.Lock()
	for {

		var tPool = make([]ServiceInfo, 0)
		self.sMap.ForEach2(sname, func(k2 ServiceInfo) bool {
			tPool = append(tPool, k2)
			return false
		})

		if len(tPool) == 0 {
			var timeLeft = stopAt.Sub(time.Now())
			if timeLeft > 0 {
				skykiss.WaitTimeout(&self.rCond, timeLeft)
				continue
			}
		} else {
			srv, found = tPool[r.Select(tPool)], true
		}
		break

	}
	self.rLock.Unlock()

	return
}

func (self *Registry) Discover(r Selector, sname string) (ServiceInfo, bool) {
	self.init()
	return self.DiscoverTimeout(r, sname, 0)
}

func (self *Registry) Sync(other *Registry, onAdd, onDelete func(string, ServiceInfo)) {
	self.init()
	other.init()
	self.sMap.Sync(
		other.sMap,
		func(k1 string, k2 ServiceInfo) {
			if onAdd != nil {
				onAdd(k1, k2)
			}
		}, func(k1 string, k2 ServiceInfo) {
			if onDelete != nil {
				onDelete(k1, k2)
			}
		},
	)
}

func (self *Registry) Iter() Iterator {
	self.init()
	return Iterator{self, 0, time.Now()}
}

func (self *Registry) Close() {
	var keep = true
	for keep {
		var last = atomic.LoadUint64(&self.rRev)
		var clean Registry
		self.Sync(&clean, nil, nil)

		var swapped = atomic.CompareAndSwapUint64(&self.rRev, last, last)
		if swapped {
			atomic.AddUint64(&self.closed, 1)
		}
		keep = atomic.LoadUint64(&self.closed) == 0
	}
	self.touch()
}
