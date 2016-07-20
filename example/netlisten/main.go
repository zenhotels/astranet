package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/zenhotels/astranet"
	"github.com/zenhotels/astranet/service"
)

var astraNet = astranet.New().Client()

func main() {
	go http.ListenAndServe("localhost:6060", nil)
	var services = astraNet.ServiceMap()

	func() {
		var rS service.Registry
		var rN = services.Iter()
		for ; ; rN.Next() {
			rS.Sync(services,
				func(_ string, s service.ServiceInfo) {
					fmt.Println("+++", s)
				},
				func(_ string, s service.ServiceInfo) {
					fmt.Println("---", s)
				},
			)
		}
	}()
}
