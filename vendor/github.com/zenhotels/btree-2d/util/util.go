package util

import (
	"hash/crc64"
	"math/rand"
	"sync"

	"github.com/zenhotels/btree-2d/uuid"
)

var machineNs = uuid.NewV1()
var idGen = rand.NewSource(int64(crc64.Checksum(machineNs.Bytes(), crc64.MakeTable(crc64.ECMA))))
var idLock sync.Mutex

func RevOffset() int64 {
	idLock.Lock()
	offset := idGen.Int63()
	idLock.Unlock()
	return offset
}
