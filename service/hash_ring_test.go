package service

import (
	"math/rand"
	"testing"

	"math"

	"hash/crc64"

	"time"

	"github.com/zenhotels/astranet/skykiss"
)

func TestHashRing(t *testing.T) {
	var hosts = make([]ServiceInfo, 0)
	var objCut uint64 = 10
	var objTotal uint64 = 1000
	var variance = 0.3
	for i := uint64(0); i < objCut; i++ {
		hosts = append(hosts, ServiceInfo{
			Host:    uint64(idGen.Int63()),
			Service: "test",
		})
	}

	// Check hash ring selector random distribution
	var hrMapHist = map[uint64]uint64{}
	for i := uint64(0); i < objTotal; i++ {
		var hr = &HashRingSelector{
			VBucket: int(rand.Int63()),
		}
		var selected = uint64(hr.Select(hosts))
		hrMapHist[selected] = hrMapHist[selected] + 1
	}
	for _, num := range hrMapHist {
		var diff = float64(num*objCut) - float64(objTotal)
		if math.Abs(diff/float64(objTotal)) > variance {
			t.Log(num, math.Abs(diff/float64(objTotal)))
			t.Fail()
		}
	}
}

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
