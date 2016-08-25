package transport

import (
	"container/list"

	"sync"

	"github.com/zenhotels/astranet/protocol"
)

const (
	CShards = 1024
)

type lockedList struct {
	*list.List
	sync.RWMutex
}

type Filter struct {
	Cmd    protocol.Command
	Remote uint64
	RPort  uint32
	Local  uint64
	LPort  uint32
}

type FiltersMap struct {
	cmd   [CShards]map[uint64]map[uint64]map[uint64]map[protocol.Command]*lockedList
	rLock [CShards]sync.RWMutex
}

func (fm *FiltersMap) set(shardId uint64, f Filter, v *lockedList) {
	var portsMap map[uint64]map[uint64]map[uint64]map[protocol.Command]*lockedList
	var remoteMap map[uint64]map[uint64]map[protocol.Command]*lockedList
	var localMap map[uint64]map[protocol.Command]*lockedList
	var commandMap map[protocol.Command]*lockedList

	if portsMap = fm.cmd[shardId]; portsMap == nil {
		portsMap = make(map[uint64]map[uint64]map[uint64]map[protocol.Command]*lockedList)
		fm.cmd[shardId] = portsMap
	}

	var tHash = (uint64(f.LPort)<<32 | uint64(f.RPort))
	if remoteMap = portsMap[tHash]; remoteMap == nil {
		remoteMap = make(map[uint64]map[uint64]map[protocol.Command]*lockedList)
		portsMap[tHash] = remoteMap
	}

	if localMap = remoteMap[f.Remote]; localMap == nil {
		localMap = make(map[uint64]map[protocol.Command]*lockedList)
		remoteMap[f.Remote] = localMap
	}

	if commandMap = localMap[f.Local]; commandMap == nil {
		commandMap = make(map[protocol.Command]*lockedList)
		localMap[f.Local] = commandMap
	}
	commandMap[f.Cmd] = v
}

func (fm *FiltersMap) get(shardId uint64, f Filter) *lockedList {
	var portsMap map[uint64]map[uint64]map[uint64]map[protocol.Command]*lockedList
	var remoteMap map[uint64]map[uint64]map[protocol.Command]*lockedList
	var localMap map[uint64]map[protocol.Command]*lockedList
	var commandMap map[protocol.Command]*lockedList

	if portsMap = fm.cmd[shardId]; portsMap == nil {
		return nil
	}

	var tHash = (uint64(f.LPort)<<32 | uint64(f.RPort))
	if remoteMap = portsMap[tHash]; remoteMap == nil {
		return nil
	}

	if localMap = remoteMap[f.Remote]; localMap == nil {
		return nil
	}

	if commandMap = localMap[f.Local]; commandMap == nil {
		return nil
	}
	return commandMap[f.Cmd]
}

func (fm *FiltersMap) delete(shardId uint64, f Filter) {
	var portsMap map[uint64]map[uint64]map[uint64]map[protocol.Command]*lockedList
	var remoteMap map[uint64]map[uint64]map[protocol.Command]*lockedList
	var localMap map[uint64]map[protocol.Command]*lockedList
	var commandMap map[protocol.Command]*lockedList

	if portsMap = fm.cmd[shardId]; portsMap == nil {
		return
	}

	var tHash = (uint64(f.LPort)<<32 | uint64(f.RPort))
	if remoteMap = portsMap[tHash]; remoteMap == nil {
		return
	}

	if localMap = remoteMap[f.Remote]; localMap == nil {
		return
	}

	if commandMap = localMap[f.Local]; commandMap == nil {
		return
	}

	delete(commandMap, f.Cmd)

	if len(commandMap) == 0 {
		delete(localMap, f.Local)
	}

	if len(localMap[f.Local]) == 0 {
		delete(remoteMap, f.Remote)
	}

	if len(remoteMap[f.Remote]) == 0 {
		delete(portsMap, tHash)
	}
}

func (fm *FiltersMap) Add(f Filter, cb Callback) *list.Element {
	var shardId = (f.LPort ^ f.RPort) % CShards
	var l = fm.rLock[shardId]

	l.Lock()
	var ll = fm.get(shardId, f)
	if ll == nil {
		ll = &lockedList{List: list.New()}
		fm.set(shardId, f, ll)
	}
	l.Unlock()

	ll.Lock()
	var el = ll.PushBack(cb)
	ll.Unlock()
	return el
}

func (fm *FiltersMap) Add(f Filter, cb Callback) *list.Element {
	var shardId = (f.LPort ^ f.RPort) % CShards
	var l = fm.rLock[shardId]

	l.Lock()
	var ll = fm.get(shardId, f)
	if ll == nil {
		ll = &lockedList{List: list.New()}
		fm.set(shardId, f, ll)
	}
	l.Unlock()

	ll.Lock()
	var el = ll.PushBack(cb)
	ll.Unlock()
	return el
}

func (fm *FiltersMap) Del(f Filter) *list.List {
	var shardId = (f.LPort ^ f.RPort) % CShards
	var l = fm.rLock[shardId]

	l.Lock()
	var ll = fm.get(shardId, f)
	if ll == nil {
		ll = &lockedList{List: list.New()}
		fm.set(shardId, f, ll)
	}
	l.Unlock()

	ll.Lock()
	var el = ll.PushBack(cb)
	ll.Unlock()
	return el
}
