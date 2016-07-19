package route

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"

	"time"

	"github.com/zenhotels/btree-2d/common"
	"stathat.com/c/consistent"
)

type Selector interface {
	Select(pool []RouteInfo) (idx int) // pool can't empty
}

type Pair struct {
	K uint64
	V RouteInfo
}

type RandomSelector struct {
}

func (RandomSelector) Select(pool []RouteInfo) (idx int) {
	return int(rand.Int31n(int32(len(pool))))
}

type HashRingSelector struct {
	VBucket int
}

func (hrs HashRingSelector) Select(pool []RouteInfo) (idx int) {
	var hr = consistent.New()
	hr.NumberOfReplicas = 1024
	var psMap = make(map[string]int, len(pool))
	for idx, p := range pool {
		var pRepr = fmt.Sprint(p)
		if _, found := psMap[pRepr]; found {
			panic("Inconsistent string repr")
		}
		psMap[pRepr] = idx
		hr.Add(pRepr)
	}
	var idKey, _ = hr.Get(strconv.Itoa(hrs.VBucket))
	return psMap[idKey]
}

type T struct {
	T uint64
}

var TCompare func(k1, k2 uint64) bool

func (self *T) Less(other common.Comparable) bool {
	return TCompare(self.T, other.(*T).T)
}

type U struct {
	U RouteInfo
}

var UCompare func(k1, k2 RouteInfo) bool

func (self *U) Less(other common.Comparable) bool {
	return UCompare(self.U, other.(*U).U)
}

type Iterator struct {
	*Registry
	last  uint64
	updAt time.Time
}

func (self Iterator) Next() Iterator {
	var last = atomic.LoadUint64(&self.rRev)
	self.rLock.Lock()
	var swapped = atomic.CompareAndSwapUint64(&self.rRev, self.last, self.last)
	var closed = atomic.LoadUint64(&self.closed)
	if swapped && closed == 0 {
		self.rCond.Wait()
	}
	self.rLock.Unlock()
	var now = time.Now()
	var timeSpent = now.Sub(self.updAt)
	if timeSpent < time.Millisecond*50 {
		time.Sleep(time.Millisecond*50 - timeSpent)
	}
	return Iterator{self.Registry, last, time.Now()}
}
