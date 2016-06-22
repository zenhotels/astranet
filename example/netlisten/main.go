package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/zenhotels/astranet"
)

var astraNet = astranet.New().Client()

func main() {
	go http.ListenAndServe("localhost:6060", nil)
	var routes = astraNet.RoutesMap()

	var rS astranet.RegistryStorage
	var rN = routes.Iter()
	for ; ; rN.Next() {
		rS.Sync(routes,
			func(s astranet.ServiceId) {
				fmt.Println("+++", s)
			},
			func(s astranet.ServiceId) {
				fmt.Println("---", s)
			},
		)
	}
}
