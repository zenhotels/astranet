package service

import (
	"hash/crc64"
	"math/rand"
	"testing"
	"time"

	"github.com/zenhotels/astranet/skykiss"
)

func BenchmarkHashRing(b *testing.B) {
	var reg Registry
	var hr = HashRingSelector{
		VBucket: 1024,
	}

	for i := 0; i < 100; i++ {
		reg.Push("test", ServiceInfo{
			Host:    uint64(idGen.Int63()),
			Service: "test",
		})
	}
	for i := 0; i < b.N; i++ {
		reg.DiscoverTimeout(hr, "test", time.Second, nil)
	}
}

var machineNs = skykiss.NewV1()
var idGen = rand.NewSource(int64(crc64.Checksum(machineNs.Bytes(), crc64.MakeTable(crc64.ECMA))))
