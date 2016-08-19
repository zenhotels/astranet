package astranet

import (
	"io"
	"net"
	"time"

	"github.com/zenhotels/astranet/protocol"
	"github.com/zenhotels/astranet/route"
	"github.com/zenhotels/astranet/service"
)

type AstraNet interface {
	DialTimeout(network string, hp string, t time.Duration) (net.Conn, error)
	Dial(network string, hp string) (net.Conn, error)
	Bind(network string, hp string) (net.Listener, error)
	Attach(conn io.ReadWriter)
	ListenAndServe(network, address string) error
	Join(network, address string) error
	Services() []service.ServiceInfo
	Routes() []route.RouteInfo
	ServiceMap() *service.Registry
	RoutesMap() *route.Registry

	WithEnv(env ...string) AstraNet
	WithLoopBack() AstraNet
	Client() AstraNet
	Server() AstraNet
	Router() AstraNet

	HttpDial(net, host string) (net.Conn, error)
}

type IOLoop struct {
	io.Reader
	io.Writer
}

type WelcomeInfo struct {
	RAddr string
}

var (
	opDial      = protocol.RegisterFrame(190, "OP_DIAL", false)
	opHandshake = protocol.RegisterFrame(191, "OP_HANDSHAKE", false)
	opService   = protocol.RegisterFrame(192, "OP_SERVICE", false)
	opNoServiсe = protocol.RegisterFrame(193, "OP_NO_SERVICE", false)
	opFollow    = protocol.RegisterFrame(194, "OP_FOLLOW", false)
	opDiscover  = protocol.RegisterFrame(195, "OP_DISCOVER", false)
	opRst       = protocol.RegisterFrame(196, "OP_RESET", false)
)
