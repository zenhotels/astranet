package service

import (
	"fmt"
	"strings"

	"github.com/zenhotels/astranet/addr"
)

type ServiceInfo struct {
	Service string
	Host    uint64
	Port    uint32
}

func (r ServiceInfo) String() string {
	return fmt.Sprintf("{%s<-%s:%d}", r.Service, addr.Uint2Host(r.Host), r.Port)
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
			return 0
		})
	}
}

//go:generate gengen github.com/zenhotels/astranet/registry string ServiceInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package registry/package service/g'"

//go:generate gengen github.com/zenhotels/btree-2d string ServiceInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package btree2d/package service/g'"

//go:generate bash -c "rm -f *.bak"
