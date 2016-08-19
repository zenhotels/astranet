package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/zenhotels/astranet"
)

var astraNet = astranet.New().Router()
var reverse = &httputil.ReverseProxy{
	Transport: &http.Transport{
		Dial: func(lnet, laddr string) (net.Conn, error) {
			var host, port, hpErr = net.SplitHostPort(laddr)
			if hpErr != nil {
				return nil, hpErr
			}
			if port != "80" {
				host += ":" + port
			}
			return astraNet.Dial(lnet, host)
		},
		DisableKeepAlives: true,
	},
	FlushInterval: time.Millisecond * 10,
	Director: func(req *http.Request) {
		for _, suffix := range []string{".p.hotcore.in", ".l.hotcore.in"} {
			if strings.HasSuffix(req.Host, suffix) {
				req.Host = req.Host[0 : len(req.Host)-len(suffix)]
			}
			if strings.HasSuffix(req.Host, suffix+":"+strconv.Itoa(*httpPort)) {
				req.Host = req.Host[0 : len(req.Host)-len(suffix+":"+strconv.Itoa(*httpPort))]
			}
		}
		req.URL.Scheme = "http"
		req.URL.Host = req.Host
	},
}

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var srvList = astraNet.Services()
	for _, srv := range srvList {
		fmt.Fprintln(w, srv)
	}
	var nodeList = astraNet.Routes()
	for _, srv := range nodeList {
		fmt.Fprintln(w, srv)
	}
}

var httpPort = flag.Int("listen", 8081, "HTTP port to bind")
var infoPort = flag.Int("info", 8080, "HTTP port to bind")

func main() {
	flag.Parse()
	if srvErr := astraNet.ListenAndServe("tcp4", "0.0.0.0:10000"); srvErr != nil {
		log.Panicln(srvErr)
	}

	router := httprouter.New()
	router.GET("/", Index)
	router.Handler("GET", "/debug/*debug", http.DefaultServeMux)

	var portStr = strconv.Itoa(*httpPort)
	astraNet.Services()

	go http.ListenAndServe("0.0.0.0:"+strconv.Itoa(*infoPort), router)

	if httpServeErr := http.ListenAndServe("0.0.0.0:"+portStr, reverse); httpServeErr != nil {
		log.Panic(httpServeErr)
	}
}
