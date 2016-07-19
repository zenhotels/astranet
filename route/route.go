package route

import (
	"math/rand"

	"fmt"
	"reflect"

	"github.com/zenhotels/astranet/addr"
	"github.com/zenhotels/astranet/transport"
)

type RouteInfo struct {
	Host     uint64
	Distance int
	Upstream transport.Transport
}

func (r RouteInfo) String() string {
	return fmt.Sprintf("{%s<-%s:%d}", addr.Uint2Host(r.Host), r.Upstream, r.Distance)
}

type RndDistSelector struct {
}

func (RndDistSelector) Select(pool []RouteInfo) int {
	var lowDist = pool[0].Distance
	var lowDistCount = 0
	for _, p := range pool {
		if p.Distance > lowDist {
			continue
		}
		if p.Distance < lowDist {
			lowDist = p.Distance
			lowDistCount = 1
			continue
		}
		lowDistCount++
	}
	var bestSeqId = int(rand.Int31n(int32(lowDistCount)))
	var iterSeqId = 0
	for idx, p := range pool {
		if p.Distance == lowDist {
			iterSeqId++
		}
		if iterSeqId == bestSeqId {
			return idx
		}
	}
	return 0
}

func init() {
	TCompare = func(k1, k2 uint64) bool {
		return k1 < k2
	}
	UCompare = func(k1, k2 RouteInfo) bool {
		if k1.Host == k2.Host {
			if k1.Distance == k2.Distance {
				var k1V = reflect.ValueOf(k1.Upstream).Elem()
				var k2V = reflect.ValueOf(k2.Upstream).Elem()
				return k1V.UnsafeAddr() < k2V.UnsafeAddr()
			}
			return k1.Distance < k2.Distance
		}
		return k1.Host < k2.Host
	}
}

//go:generate gengen github.com/zenhotels/astranet/registry uint64 RouteInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package registry/package route/g'; rm *.bak"
