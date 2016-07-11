package service

type ServiceInfo struct {
	Service string
	Host    uint64
	Port    uint32
}

//go:generate gengen github.com/zenhotels/astranet/registry string ServiceInfo
//go:generate bash -c "ls | xargs -n1 sed -i .bak 's/^package registry/package service/g'; rm *.bak"
