package service

import (
	"fmt"

	"github.com/zenhotels/astranet/addr"
	"github.com/zenhotels/astranet/transport"
)

type ServiceInfo struct {
	Service  string
	Host     uint64
	Port     uint32
	Priority int
	Upstream transport.Transport
}

func (r ServiceInfo) String() string {
	var faraway = "faraway;"
	if r.Priority == 0 {
		faraway = ""
	}
	return fmt.Sprintf("{%s<-%s:%d;%s%v}", r.Service, addr.Uint2Host(r.Host), r.Port, faraway, r.Upstream)
}

func init() {
	BTreeNew = func() BTree2D {
		return New()
	}
}

type uniqueHostPort struct{}

func (uniqueHostPort) Reduce(pool []ServiceInfo) []ServiceInfo {
	if len(pool) == 0 {
		return pool
	}
	var res = make([]ServiceInfo, 0, len(pool))
	res = append(res, pool[0])
	for i := 1; i < len(pool); i++ {
		var t, k = pool[i], res[len(res)-1]
		if t.Host == k.Host && t.Port == k.Port {
			continue
		}
		res = append(res, t)
	}
	return res
}

var UniqueHP = uniqueHostPort{}

//go:generate gengen github.com/zenhotels/astranet/registry string ServiceInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package registry/package service/g'"

//go:generate gengen github.com/zenhotels/dumbmap-2d string ServiceInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package dumbmap2d/package service/g'"

//go:generate bash -c "rm -f *.bak"
