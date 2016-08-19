package transport

import (
	"time"

	"net"

	"github.com/zenhotels/astranet/protocol"
)

type Filter struct {
	Cmd    protocol.Command
	Remote uint64
	RPort  uint32
	Local  uint64
	LPort  uint32
}

type Handler interface {
	Close() // Close() can be safely called multiple times
}

type Callback func(op protocol.Op, upstream Transport)

type Transport interface {
	String() string
	SendTimeout(op protocol.Op, t time.Duration) error
	Send(op protocol.Op) error
	Queue(op protocol.Op)
	Handle(cb Callback, filters ...Filter) Handler
	RAddr() net.Addr
	Join()
	Close()
	Drain()
	IsClosed() bool
	OnClose(func(u Transport))
}

const (
	WND_SIZE = 32 * 1024
)

var opPing = protocol.RegisterFrame(160, "OP_PING", false)
var opPong = protocol.RegisterFrame(161, "OP_PONG", false)
