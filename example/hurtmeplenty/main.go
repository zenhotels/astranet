package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync/atomic"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/zenhotels/astranet"
)

var astraNet = astranet.New()

func main() {
	var lErr = astraNet.ListenAndServe("tcp4", ":19080")
	if lErr != nil {
		panic(lErr)
	}

	var httpdL, httpdLErr = astraNet.Bind("tcp4", "httpserv")
	if httpdLErr != nil {
		panic(httpdLErr)
	}

	var cskynet = astranet.New()
	var bindErr = cskynet.Join("tcp4", "localhost:19080")
	if bindErr != nil {
		panic(bindErr)
	}

	var streams int64 = 0
	var tail int64 = 0

	t := metrics.NewTimer()
	metrics.Register("connection time", t)

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, req *http.Request) {
		atomic.AddInt64(&streams, 1)
		w.(http.Flusher).Flush()
		time.Sleep(time.Duration(rand.Int63n(10000)) * time.Millisecond)
	})

	go http.Serve(httpdL, mux)
	go http.ListenAndServe("localhost:6060", nil)

	var cHttp = &http.Client{
		Transport: &http.Transport{
			Dial: func(lnet, laddr string) (net.Conn, error) {
				var host, port, hpErr = net.SplitHostPort(laddr)
				if hpErr != nil {
					return nil, hpErr
				}
				if port != "80" {
					host += ":" + port
				}
				return cskynet.Dial(lnet, host)
			},
		},
		Timeout: 10 * time.Second,
	}
	client := func() {

		var httpdL, httpdLErr = astraNet.Bind("", ":0")
		if httpdLErr != nil {
			panic(httpdLErr)
		}
		go http.Serve(httpdL, mux)
		go func() {
			time.Sleep(time.Second)
			httpdL.Close()
		}()

		var before = time.Now()
		var cResp, cErr = cHttp.Get("http://httpserv/ping")
		var after = time.Now()
		if cErr != nil {
			fmt.Println(cErr, after.Sub(before))
			return
		}
		defer cResp.Body.Close()
		t.Update(after.Sub(before) / time.Millisecond)
		atomic.AddInt64(&tail, -1)
		_, cErr = io.Copy(ioutil.Discard, cResp.Body)
		atomic.AddInt64(&streams, 1)
		after = time.Now()
		if cErr != nil {
			fmt.Println(cErr, after.Sub(before))
		}
	}

	go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	for i := 0; i < 32*1024*10; i++ {
		atomic.AddInt64(&tail, 1)
		go client()
		time.Sleep(10 * time.Duration(rand.Int63n(10)) * time.Microsecond)
		if i%100 == 0 {
			fmt.Println(i, atomic.LoadInt64(&tail))
		}
	}
	for i := 0; i < 600; i++ {
		time.Sleep(time.Second)
		fmt.Println(i, tail)
	}
}
