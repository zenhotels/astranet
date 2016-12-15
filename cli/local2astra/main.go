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
)

var httpPort = flag.Int("port", 8080, "HTTP port of application to publish")
var bind = flag.String("bind", "0.0.0.0:10000", "TCP address to bind astranet engine to")
var appName = flag.String("name", uuid.NewV4().String(), "name of application to publish")
var cmd = flag.String("cmd", "", "command to execute (you can start your server here)")

func main() {
	flag.Parse()
	var astraNet = astranet.New().Server()
	astraNet.ListenAndServe("", *bind)
	var fwdService, fwdErr = astraNet.Bind("", *appName)
	if fwdErr != nil {
		log.Panicln(fwdService)
	}

	var transport = &http.Transport{
		Dial: func(lnet, laddr string) (net.Conn, error) {
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

	if len(*cmd) > 0 {
		go http.Serve(fwdService, forwarder)
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
	}
}