package transport

import (
	"container/list"
	"sync"

	"github.com/zenhotels/astranet/protocol"
)

type Router struct {
	cbLock  sync.RWMutex
	cbMap   map[Filter]*list.List
	initCtl sync.Once
}

func (self *Router) init() {
	self.initCtl.Do(func() {
		self.cbMap = make(map[Filter]*list.List)
	})
}

func (self *Router) Handle(cb Callback, filters ...Filter) Handler {
	self.init()
	var eList = make(map[Filter]*list.Element, len(filters))
	for _, f := range filters {
		if eList[f] != nil {
			continue
		}

		self.cbLock.Lock()
		var l = self.cbMap[f]
		if l == nil {
			l = list.New()
			self.cbMap[f] = l
		}

		eList[f] = l.PushBack(cb)
		self.cbLock.Unlock()
	}
	return &handler{cb, eList, self, false}
}

func (self *Router) CloseHandle(filter Filter, el *list.Element) {
	self.cbLock.Lock()
	var eMap = self.cbMap[filter]
	if eMap != nil {
		eMap.Remove(el)
		if eMap.Len() == 0 {
			delete(self.cbMap, filter)
		}
	}
	self.cbLock.Unlock()
}

func (self *Router) checkCb(f Filter) (cb Callback) {
	self.cbLock.RLock()
	if cbL, found := self.cbMap[f]; found && cbL.Len() > 0 {
		cb = cbL.Back().Value.(Callback)
	}
	self.cbLock.RUnlock()
	return cb
}

func (self *Router) Check(fL ...Filter) (cb Callback) {
	self.init()
	for _, f := range fL {
		if cb = self.checkCb(f); cb != nil {
			return cb
		}
	}
	return
}

func (self *Router) CheckFrame(job protocol.Op) (cb Callback) {
	return self.Check(Filter{
		Cmd:    job.Cmd,
		Remote: job.Remote,
		RPort:  job.RPort,
		Local:  job.Local,
		LPort:  job.LPort,
	}, Filter{
		Cmd:    job.Cmd,
		Remote: job.Remote,
		RPort:  job.RPort,
	}, Filter{
		Cmd: job.Cmd,
	}, Filter{})
}
