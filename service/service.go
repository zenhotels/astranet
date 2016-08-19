package service

import (
	"fmt"

	"strings"

	"reflect"

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
		return New(func(k1, k2 string) int {
			return strings.Compare(k1, k2)
		}, func(k1, k2 ServiceInfo) int {
			if k1.Service < k2.Service {
				return -1
			}
			if k1.Service > k2.Service {
				return 1
			}
			if k1.Host < k2.Host {
				return -1
			}
			if k1.Host > k2.Host {
				return 1
			}
			if k1.Port < k2.Port {
				return -1
			}
			if k1.Port > k2.Port {
				return 1
			}
			if k1.Priority < k2.Priority {
				return -1
			}
			if k1.Priority > k2.Priority {
				return 1
			}
			var k1V uintptr = 0
			var k2V uintptr = 0
			if k1.Upstream != nil {
				k1V = reflect.ValueOf(k1.Upstream).Elem().UnsafeAddr()
			}
			if k2.Upstream != nil {
				k2V = reflect.ValueOf(k2.Upstream).Elem().UnsafeAddr()
			}
			if k1V < k2V {
				return -1
			}
			if k1V > k2V {
				return 1
			}
			return 0
		})
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

//go:generate gengen github.com/zenhotels/btree-2d string ServiceInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package btree2d/package service/g'"

//go:generate bash -c "rm -f *.bak"
