package astranet

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ServicePool struct {
	srvMap map[ServiceId][]func()
	count  int
	rLock  sync.RWMutex
}

type ServiceId struct {
	Service  string
	Priority int
	Host     uint64
	Port     uint32
	Upstream *mpxRemote
}

type StorageIterator struct {
	*RegistryStorage
	last uint64
}

type ServiceList []ServiceId

func (l ServiceList) Len() int {
	return len(l)
}

func (l ServiceList) Less(i, j int) bool {
	if l[i].Priority == l[j].Priority {
		return l[i].Host < l[j].Host
	}
	return l[i].Priority < l[j].Priority
}

func (l ServiceList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type RouteChooser interface {
	Select(pool []ServiceId, vbucket int) (idx int)
}

type RandomChooser struct {
}

func (RandomChooser) Select(pool []ServiceId, vbucket int) (idx int) {
	sort.Sort(ServiceList(pool))
	var bestPriority = pool[0].Priority
	var bestPriorityCount = 0
	for idx, pi := range pool {
		if pi.Priority == bestPriority {
			bestPriorityCount = idx + 1
		}
	}
	return int(rand.Int63()) % len(pool[0:bestPriorityCount])
}

type SortedChooser struct {
}

func (SortedChooser) Select(pool []ServiceId, vbucket int) (idx int) {
	sort.Sort(ServiceList(pool))
	return int(rand.New(rand.NewSource(int64(vbucket))).Int63()) % len(pool)
}

func (self StorageIterator) Next() StorageIterator {
	var last = atomic.LoadUint64(&self.rRev)
	self.rLock.RLock()
	var swapped = atomic.CompareAndSwapUint64(&self.rRev, self.last, self.last)
	var closed = self.closed
	if swapped && !closed {
		self.rCond.Wait()
	}
	self.rLock.RUnlock()
	return StorageIterator{self.RegistryStorage, last}
}

type RegistryStorage struct {
	sMap    map[string]*ServicePool
	rLock   sync.RWMutex
	rCond   sync.Cond
	rRev    uint64
	closed  bool
	initCtl sync.Once
}

func (self *RegistryStorage) init() {
	self.initCtl.Do(func() {
		self.sMap = make(map[string]*ServicePool)
		self.rCond.L = self.rLock.RLocker()
	})
}

func (self *RegistryStorage) touch() {
	self.rLock.Lock()
	atomic.AddUint64(&self.rRev, 1)
	self.rLock.Unlock()
	self.rCond.Broadcast()
}

func (self *RegistryStorage) push(srv ServiceId, action ...func()) {
	self.init()
	self.rLock.Lock()
	var closed = self.closed
	if self.sMap[srv.Service] == nil {
		self.sMap[srv.Service] = new(ServicePool)
	}
	var servicePool = self.sMap[srv.Service]
	self.rLock.Unlock()
	if closed {
		return
	}

	servicePool.rLock.Lock()
	if servicePool.srvMap == nil {
		servicePool.srvMap = make(map[ServiceId][]func())
	}
	servicePool.srvMap[srv] = append(servicePool.srvMap[srv], action...)
	servicePool.rLock.Unlock()

	self.touch()
}

func (self *RegistryStorage) Push(srv ServiceId, action ...func()) {
	self.push(srv, action...)
}

func (self *RegistryStorage) Pop(srv ServiceId) {
	self.init()
	self.rLock.Lock()
	if self.sMap[srv.Service] == nil {
		self.sMap[srv.Service] = new(ServicePool)
	}
	var servicePool = self.sMap[srv.Service]
	self.rLock.Unlock()

	servicePool.rLock.Lock()
	for _, actionList := range servicePool.srvMap {
		for _, action := range actionList {
			go action()
		}
	}
	delete(servicePool.srvMap, srv)
	servicePool.rLock.Unlock()
	self.touch()
}

func (self *RegistryStorage) DiscoverTimeout(
	r RouteChooser, vbucket int,
	sname string, wait time.Duration,
) (srv *ServiceId) {
	self.init()
	//mpxLog.Println("RegistryStorage::DiscoverTimeout::enter", sname, wait)
	var started = time.Now()
	var now = started
	if wait == 0 {
		wait = time.Second
	}
	for srv == nil && started.Add(wait).After(now) {
		self.rLock.RLock()
		var servicePool = self.sMap[sname]
		servicePool = self.sMap[sname]
		self.rLock.RUnlock()

		if servicePool != nil {
			servicePool.rLock.RLock()
			var srvPool = make([]ServiceId, 0, len(servicePool.srvMap))
			for srvL := range servicePool.srvMap {
				srvPool = append(srvPool, srvL)
			}
			var hostsMet = map[uint64]bool{}
			var dedupedSrv = make([]ServiceId, 0, len(servicePool.srvMap))
			sort.Sort(ServiceList(srvPool))
			for _, srv := range srvPool {
				if hostsMet[srv.Host] {
					continue
				}
				dedupedSrv = append(dedupedSrv, srv)
				hostsMet[srv.Host] = true
			}
			if len(srvPool) > 0 {
				srv = new(ServiceId)
				*srv = dedupedSrv[r.Select(dedupedSrv, vbucket)]
			}
			servicePool.rLock.RUnlock()
		}

		if srv == nil {
			time.Sleep(time.Millisecond * 10)
			now = time.Now()
			continue
		}
	}
	//mpxLog.Println("RegistryStorage::DiscoverTimeout::done", sname, srv == nil)
	return
}

func (self *RegistryStorage) Discover(r RouteChooser, vbucket int, sname string) (srv *ServiceId) {
	self.init()
	return self.DiscoverTimeout(r, vbucket, sname, time.Second)
}

func (self *RegistryStorage) Sync(other *RegistryStorage, onAdd, onDelete func(ServiceId)) {
	self.init()
	other.init()
	var added, deleted []ServiceId
	self.rLock.RLock()
	other.rLock.RLock()
	for service, sPool := range self.sMap {
		sPool.rLock.RLock()
		for s := range sPool.srvMap {
			var sMap = other.sMap[service]
			if sMap == nil {
				deleted = append(deleted, s)
				continue
			}
			sMap.rLock.RLock()
			if _, found := sMap.srvMap[s]; !found {
				deleted = append(deleted, s)
			}
			sMap.rLock.RUnlock()
		}
		sPool.rLock.RUnlock()
	}
	for service, sPool := range other.sMap {
		sPool.rLock.RLock()
		for s := range sPool.srvMap {
			var sMap = self.sMap[service]
			if sMap == nil {
				added = append(added, s)
				continue
			}
			sMap.rLock.RLock()
			if _, found := sMap.srvMap[s]; !found {
				added = append(added, s)
			}
			sMap.rLock.RUnlock()
		}
		sPool.rLock.RUnlock()
	}
	other.rLock.RUnlock()
	self.rLock.RUnlock()

	for _, add := range added {
		self.Push(add)
		if onAdd != nil {
			onAdd(add)
		}
	}
	for _, del := range deleted {
		self.Pop(del)
		if onDelete != nil {
			onDelete(del)
		}
	}
}

func (self *RegistryStorage) Iter() StorageIterator {
	self.init()
	return StorageIterator{self, 0}
}

func (self *RegistryStorage) Close() {
	var keep = true
	for keep {
		var last = atomic.LoadUint64(&self.rRev)
		var clean RegistryStorage
		self.Sync(&clean, func(ServiceId) {}, func(s ServiceId) {})

		self.rLock.Lock()
		var swapped = atomic.CompareAndSwapUint64(&self.rRev, last, last)
		if swapped {
			self.closed = true
		}
		keep = !self.closed
		self.rLock.Unlock()

	}
	self.touch()
}
