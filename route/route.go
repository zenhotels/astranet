package route

import (
	"math/rand"

	"fmt"

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

func (RndDistSelector) Select(pool []RouteInfo) (int, bool) {
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
			return idx, false
		}
	}
	return 0, false
}

func init() {
	BTreeNew = func() BTree2D {
		return New()
	}
}

//go:generate gengen github.com/zenhotels/astranet/registry uint64 RouteInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package registry/package route/g'"

//go:generate gengen github.com/zenhotels/dumbmap-2d uint64 RouteInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package dumbmap2d/package route/g'"

//go:generate bash -c "rm -f *.bak"
