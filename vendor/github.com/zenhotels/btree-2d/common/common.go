package common

import (
	"hash/crc64"
	"math/rand"
	"sync"

	"github.com/zenhotels/btree-2d/uuid"
)

type Comparable interface {
	Less(other Comparable) bool
}

type FinalizableComparable interface {
	Comparable

	Value() Comparable
	Finalize()
	AddFinalizer(fn func()) bool
}

var machineNs = uuid.NewV1()
var idGen = rand.NewSource(int64(crc64.Checksum(machineNs.Bytes(), crc64.MakeTable(crc64.ECMA))))
var idLock sync.Mutex

func RevOffset() int64 {
	idLock.Lock()
	offset := idGen.Int63()
	idLock.Unlock()
	return offset
}
