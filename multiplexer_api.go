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

	HttpDial(net, host string) (net.Conn, error)
}

type IOLoop struct {
	io.Reader
	io.Writer
}

var (
	opNew       = protocol.RegisterFrame(1, "OP_NEW", false)
	opDiscover  = protocol.RegisterFrame(196, "OP_DISCOVER", false)
	opForget    = protocol.RegisterFrame(197, "OP_FORGET", false)
	opService   = protocol.RegisterFrame(198, "OP_SERVICE", false)
	opNoServiсe = protocol.RegisterFrame(199, "OP_NO_SERVICE", false)
	opJoinMe    = protocol.RegisterFrame(201, "OP_JOIN_ME", false)
	opRHost     = protocol.RegisterFrame(202, "OP_RHOST", false)
	opHandshake = protocol.RegisterFrame(203, "OP_HANDSHAKE", false)
)
