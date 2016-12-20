// local2astra is a cli tool used to publish non-astranet code to astra network
package main

import (
)
import (
	"flag"
	"log"
	"net/http"
	"net"
	"os/exec"
	"os"
	"strconv"
	"time"

	"github.com/zenhotels/btree-2d/uuid"
	"github.com/zenhotels/astranet"
	"github.com/vulcand/oxy/forward"
	"fmt"
	"os/signal"
	"syscall"
	"strings"
	"github.com/vulcand/oxy/testutils"
)

var httpPort = flag.Int("port", 8080, "HTTP port of application to publish")
var bind = flag.String("bind", "0.0.0.0:10000", "TCP address to bind astranet engine to")
var appName = flag.String("name", uuid.NewV4().String(), "name of application to publish")
var cmd = flag.String("cmd", "", "command to execute (you can start your server here)")
var check_url = flag.String("check_url", "/", "Url to check the service for to treat as live")
var timeout = flag.Duration("timeout", time.Millisecond*100, "timeout for URL response")
var retry = flag.Duration("retry", time.Millisecond*1000, "duration for retry FAILED URL response")
var recheck = flag.Duration("recheck", time.Millisecond*100, "duration for retry OK URL response")
var shutdown_timeout = flag.Duration("wait", time.Second*20, "duration to wait before handle shut down")

func checker(ready_ch, close_ch chan bool, url string) {
	var client = &http.Client{
		Timeout: *timeout,
	}

	var ready bool
	for {
		var resp, respErr = client.Get(url)
		if respErr != nil {
			log.Printf("HTTP GET Failure %v\n", respErr)
		}
		if respErr == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				if !ready {
					ready = true
					close(ready_ch)
				}
				time.Sleep(*recheck)
				continue
			}
			log.Printf("HTTP GET Bad Status %v\n", resp.Status)
		}
		if ready {
			close(close_ch)
			break
		}
		time.Sleep(*retry)
	}
}

func main() {
	flag.Parse()
	log.Println(timeout, retry, recheck)
	var astraNet = astranet.New().Server()
	astraNet.ListenAndServe("", *bind)

	var shutdown = make(chan struct{})
	var signal_ch = make(chan os.Signal, 2)
	signal.Notify(signal_ch, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signal_ch
		close(shutdown)
	}()

	var transport = &http.Transport{
		Dial: func(lnet, _ string) (net.Conn, error) {
			return net.Dial(lnet, ":"+strconv.Itoa(*httpPort))
		},
		DisableKeepAlives:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	var forwarder, _ = forward.New(
		forward.PassHostHeader(true),
		forward.RoundTripper(transport),
	)
	var fwd_func = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var query = req.RequestURI
		if !strings.HasPrefix(query, "/") {
			query = "/" + query
		}
		req.URL = testutils.ParseURI("http://" + req.Host + query)
		req.RequestURI = query
		forwarder.ServeHTTP(w, req)
	})

	var binder = func(ready_ch, close_ch chan bool) {
		select {
		case <-ready_ch:
			var fwdService, fwdErr = astraNet.Bind("", *appName)
			if fwdErr != nil {
				log.Panicln(fwdService)
			}
			go http.Serve(fwdService, fwd_func)
			defer fwdService.Close()
		case <-close_ch:
		case <-shutdown:
		}
		select {
		case <-close_ch:
		case <-shutdown:
		}
	}
	go func() { //bind_loop
		var check_url = fmt.Sprintf("http://127.0.0.1:%d%s", *httpPort, *check_url)
		for {
			var ready_ch = make(chan bool, 0)
			var close_ch = make(chan bool, 0)
			go checker(ready_ch, close_ch, check_url)
			binder(ready_ch, close_ch)

			select {
			case <-shutdown:
				time.Sleep(*shutdown_timeout)
				os.Exit(0)
			default:
			}
		}
	}()

	if len(*cmd) > 0 {
		var command = exec.Cmd{
			Path:   "/bin/bash",
			Args:   []string{"bash", "-c", *cmd},
			Stdout: os.Stdout,
			Stdin:  os.Stdin,
			Stderr: os.Stderr,
		}
		var runErr = command.Run()
		if runErr != nil {
			log.Panic(runErr)
		}
	} else {
		var hung chan bool
		<-hung
	}
}