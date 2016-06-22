package astranet

import (
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var ErrInvalidSyn = errors.New("Invalid connect message")

type Listener struct {
	network  string
	lport    uint32
	hostname string
	hostport string
	postfix  string

	opQueue []Op
	opLock  sync.Mutex
	opNew   sync.Cond

	closed  uint32
	service ServiceId

	mpx *multiplexer
}

func (self *Listener) init() *Listener {
	self.opNew.L = &self.opLock
	self.service = ServiceId{
		Service: self.hostname + self.postfix,
		Host:    self.mpx.local,
		Port:    self.lport,
	}
	if self.hostname != "" {
		self.mpx.services.Push(self.service)
	}
	return self
}

func (self *Listener) recv(op Op) {
	self.opLock.Lock()
	var closed = atomic.LoadUint32(&self.closed)
	if closed == 0 {
		self.opQueue = append(self.opQueue, op)
	}
	self.opLock.Unlock()
	self.opNew.Broadcast()
}

func (self *Listener) Close() error {
	self.mpx.bLock.Lock()
	delete(self.mpx.binds, self.lport)
	atomic.StoreUint32(&self.closed, 1)
	self.mpx.bLock.Unlock()
	if self.hostname != "" {
		self.mpx.services.Pop(self.service)
	}
	self.opNew.Broadcast()
	return nil
}

func AcceptTimeout(l net.Listener, t time.Duration) (conn net.Conn, err error) {
	var rdy = make(chan struct{})
	var timeout = time.NewTimer(t)
	go func() {
		conn, err = l.Accept()
		close(rdy)
	}()
	select {
	case <-rdy:
	case <-timeout.C:
		err = io.ErrNoProgress
		l.Close()
	}
	return
}

func (self *Listener) Accept() (conn net.Conn, err error) {
	var op Op
	self.opLock.Lock()
	for {
		var closed = atomic.LoadUint32(&self.closed)
		if closed > 0 {
			err = io.EOF
			break
		}
		if len(self.opQueue) > 0 {
			op = self.opQueue[0]
			copy(self.opQueue[:], self.opQueue[1:])
			self.opQueue = self.opQueue[0 : len(self.opQueue)-1]
			break
		}
		self.opNew.Wait()
	}
	self.opLock.Unlock()
	if err != nil {
		return nil, err
	}
	if op.Cmd != OP_NEW {
		return nil, ErrInvalidSyn
	}
	return makeDeferredConn(func() (net.Conn, error) {
		return self.mpx.dialSrv(
			self.network,
			net.JoinHostPort(
				Uint2Host(op.Remote),
				strconv.FormatUint(uint64(op.RPort), 10),
			),
			op.Route,
		)
	}), nil
}

func (self *Listener) Addr() net.Addr {
	return &Addr{self.network, self.mpx.local, self.lport}
}
