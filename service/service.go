package service

import "strings"

type ServiceInfo struct {
	Service string
	Host    uint64
	Port    uint32
}

func init() {
	TCompare = func(k1, k2 string) bool {
		return strings.Compare(k1, k2) == -1
	}
	UCompare = func(k1, k2 ServiceInfo) bool {
		if k1.Service == k2.Service {
			if k1.Host == k2.Host {
				return k1.Port < k2.Port
			}
			return k1.Host < k2.Host
		}
		return strings.Compare(k1.Service, k2.Service) == -1
	}
}

//go:generate gengen github.com/zenhotels/astranet/registry string ServiceInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package registry/package service/g'; rm *.bak"
