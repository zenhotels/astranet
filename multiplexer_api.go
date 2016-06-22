package astranet

import (
	"io"
	"net"
	"time"
)

type AstraNet interface {
	DialTimeout(network string, hp string, t time.Duration) (net.Conn, error)
	Dial(network string, hp string) (net.Conn, error)
	Bind(network string, hp string) (net.Listener, error)
	Attach(conn io.ReadWriter)
	ListenAndServe(network, address string) error
	Join(network, address string) error
	Services() []ServiceId
	Routes() []ServiceId
	ServiceMap() *RegistryStorage
	RoutesMap() *RegistryStorage

	WithEnv(env ...string) AstraNet
	WithLoopBack() AstraNet
	Client() AstraNet
	Server() AstraNet
	New() AstraNet

	HttpDial(net, host string) (net.Conn, error)
}
