package listener

import (
	"io"
	"net"
	"sync/atomic"

	"sync"

	"github.com/zenhotels/astranet/addr"
	"github.com/zenhotels/astranet/protocol"
	"github.com/zenhotels/astranet/service"
	"github.com/zenhotels/astranet/socket"
	"github.com/zenhotels/astranet/transport"
)

type NewConnRequest struct {
	Transport transport.Transport
	HostId    uint64
	PortId    uint32
}

type Listener struct {
	service.ServiceInfo

	network string
	opChan  []NewConnRequest
	opLock  sync.Mutex
	opNew   sync.Cond

	closed  uint32
	onClose []func()
}

func New(network string, hostId uint64, lPort uint32, service string) *Listener {
	var self = &Listener{}
	self.ServiceInfo.Host = hostId
	self.ServiceInfo.Port = lPort
	self.ServiceInfo.Service = service
	self.network = network
	self.opNew.L = &self.opLock
	return self
}

func (self *Listener) OnClose(c func()) {
	self.onClose = append(self.onClose, c)
}

func (self *Listener) Close() error {
	var cLock = atomic.AddUint32(&self.closed, 1)
	self.opNew.Broadcast()
	if cLock == 1 {
		for _, c := range self.onClose {
			c()
		}
	}
	return nil
}

func (self *Listener) Recv(op protocol.Op, upstream transport.Transport) {
	var cr = NewConnRequest{upstream, op.Local, op.LPort}
	self.opLock.Lock()
	self.opChan = append(self.opChan, cr)
	self.opNew.Broadcast()
	self.opLock.Unlock()
}

func (self *Listener) Accept() (net.Conn, error) {
	var op NewConnRequest
	self.opLock.Lock()
	for atomic.LoadUint32(&self.closed) == 0 {
		if len(self.opChan) == 0 {
			self.opNew.Wait()
			continue
		}
		op = self.opChan[0]
		var n = copy(self.opChan[0:], self.opChan[1:])
		self.opChan = self.opChan[0:n]
		break
	}
	self.opLock.Unlock()
	if op.Transport == nil {
		self.Close()
		return nil, io.EOF
	}
	return socket.NewServerSocket(
		self.network, self.ServiceInfo.Host, self.ServiceInfo.Port,
		op.HostId, op.PortId, op.Transport,
	), nil
}

func (self *Listener) Addr() net.Addr {
	return &addr.Addr{self.network, self.ServiceInfo.Host, self.ServiceInfo.Port}
}
