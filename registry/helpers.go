package registry

import (
	"sync"

	"fmt"
	"math/rand"
	"strconv"

	"github.com/joeshaw/gengen/generic"
	"stathat.com/c/consistent"
)

type Pool struct {
	srvMap  map[generic.U][]func()
	version uint64
	rLock   sync.RWMutex
}

type Selector interface {
	Select(pool []generic.U) (idx int) // pool can't empty
}

type Pair struct {
	K generic.T
	V generic.U
}

type RandomSelector struct {
}

func (RandomSelector) Select(pool []generic.U) (idx int) {
	return int(rand.Int31n(int32(len(pool))))
}

type HashRingSelector struct {
	VBucket int
}

func (hrs HashRingSelector) Select(pool []generic.U) (idx int) {
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
