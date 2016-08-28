package route

import (
	"math/rand"
	"sync/atomic"

	"time"

	"fmt"
	"strconv"

	"github.com/serialx/hashring"
)

var BTreeNew func() BTree2D

type Iterator struct {
	*Registry
	last  uint64
	updAt time.Time
}

func (self Iterator) Next() Iterator {
	var last = atomic.LoadUint64(&self.rRev)
	self.rLock.RLock()
	var swapped = atomic.CompareAndSwapUint64(&self.rRev, self.last, self.last)
	var closed = atomic.LoadUint64(&self.closed)
	if swapped && closed == 0 {
		self.rCond.Wait()
	}
	self.rLock.RUnlock()
	var now = time.Now()
	var timeSpent = now.Sub(self.updAt)
	if timeSpent < time.Millisecond*50 {
		time.Sleep(time.Millisecond*50 - timeSpent)
	}
	return Iterator{self.Registry, last, time.Now()}
}

type Pair struct {
	K uint64
	V RouteInfo
}

type Selector interface {
	Select(pool []RouteInfo) (idx int) // pool can't empty
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
	if hrs.VBucket == 0 {
		return int(rand.Int31n(int32(len(pool))))
	}

	var sPool = make([]string, len(pool))
	var node2Idx = make(map[string]int, len(pool))
	for idx, p := range pool {
		sPool[idx] = fmt.Sprint(p)
		node2Idx[sPool[idx]] = idx
	}
	var c = hashring.New(sPool)
	var selected, _ = c.GetNode(strconv.Itoa(hrs.VBucket))
	return node2Idx[selected]
}

type Reducer interface {
	Reduce(pool []RouteInfo) []RouteInfo
}
