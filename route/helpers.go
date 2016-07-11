package route

import (
	"sync"

	"fmt"
	"math/rand"
	"strconv"

	"stathat.com/c/consistent"
)

type Pool struct {
	srvMap  map[RouteInfo][]func()
	version uint64
	rLock   sync.RWMutex
}

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
