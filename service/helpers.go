package service

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"

	"time"

	"stathat.com/c/consistent"
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
	K string
	V ServiceInfo
}

type Selector interface {
	Select(pool []ServiceInfo) (idx int) // pool can't empty
}

type RandomSelector struct {
}

func (RandomSelector) Select(pool []ServiceInfo) (idx int) {
	return int(rand.Int31n(int32(len(pool))))
}

type HashRingSelector struct {
	VBucket int
}

func (hrs HashRingSelector) Select(pool []ServiceInfo) (idx int) {
	if hrs.VBucket == 0 {
		return int(rand.Int31n(int32(len(pool))))
	}

	var hr, psMap = consistent.New(), make(map[string]int, len(pool))
	hr.NumberOfReplicas = 1024
	var repr = make([]string, 0, len(pool))
	for idx, p := range pool {
		var pS = fmt.Sprint(p)
		repr = append(repr, pS)
		psMap[pS] = idx
	}
	hr.Set(repr)

	var idKey, _ = hr.Get(strconv.Itoa(hrs.VBucket))
	return psMap[idKey]
}

type Reducer interface {
	Reduce(pool []ServiceInfo) []ServiceInfo
}
