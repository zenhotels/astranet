package socket

import (
	"io"
	"testing"

	"io/ioutil"
	"log"
	"time"

	"os"

	"github.com/zenhotels/astranet/glog"
	"github.com/zenhotels/astranet/skykiss"
	"github.com/zenhotels/astranet/transport"
)

func TestUpstreamClose(t *testing.T) {
	var msg = make([]byte, 1024*1024)
	var r, w = io.Pipe()
	var ioLoop = skykiss.IOLoop{r, w}
	var l = glog.New(0, log.New(os.Stderr, "TestUpstreamClose", log.LstdFlags))

	var upstream = transport.Upstream(ioLoop, l, nil, time.Minute)
	var client = NewClientSocket("", 1, 1, upstream)
	var server = NewServerSocket("", 2, 2, 1, 1, upstream)
	go func() {
		server.Write(msg)
		server.Close()
	}()
	var buf, _ = ioutil.ReadAll(client)
	if len(buf) != len(msg) {
		t.Fail()
	}
}

func BenchmarkUpstreamClose(b *testing.B) {
	var msg = make([]byte, 1024*b.N)
	var r, w = io.Pipe()
	var ioLoop = skykiss.IOLoop{r, w}
	var l = glog.New(0, log.New(os.Stderr, "TestUpstreamClose", log.LstdFlags))

	var upstream = transport.Upstream(ioLoop, l, nil, time.Minute)
	var client = NewClientSocket("", 1, 1, upstream)
	var server = NewServerSocket("", 2, 2, 1, 1, upstream)
	go func() {
		server.Write(msg)
		server.Close()
	}()
	var buf, _ = ioutil.ReadAll(client)
	if len(buf) != len(msg) {
		b.Fail()
	}
	b.SetBytes(1024)
}
