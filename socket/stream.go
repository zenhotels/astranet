package socket

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/zenhotels/astranet/addr"
	"github.com/zenhotels/astranet/protocol"
	"github.com/zenhotels/astranet/skykiss"
	"github.com/zenhotels/astranet/transport"
)

const (
	TCP_CLOSE_WND = 10 * time.Second
)

type stream struct {
	network string

	local  uint64
	remote uint64
	lport  uint32
	rport  uint32

	canSend uint32
	status  ConnState
	state   sync.Cond
	sLock   sync.RWMutex

	rBuf    []skykiss.BytesPackage
	wBuf    uint32
	wndSize uint32

	connHndl transport.Handler

	mpx       transport.Transport
	rDeadLine time.Time
	wDeadLine time.Time
}

func (self *stream) Join(op ConnState) (state ConnState) {
	var match = func(op ConnState) bool {
		return (self.status & op) == op
	}
	self.sLock.Lock()
state_loop:
	for {
		switch {
		case !self.rDeadLine.IsZero() && self.rDeadLine.Before(time.Now()):
			self.status = self.status | stForceClosed
			break state_loop
		case match(op):
			break state_loop
		case match(stClosed):
			break state_loop
		default:
			self.state.Wait()
		}
	}
	state = self.status
	self.sLock.Unlock()
	return
}

func (self *stream) recv(op protocol.Op, _ transport.Transport) {
	self.sLock.Lock()

	switch op.Cmd {
	case opSyn:
		self.status = self.status | stSyn
		self.remote = op.Local
		self.rport = op.LPort
		self.state.Broadcast()
		self.sLock.Unlock()
		self.subscribe(opSynAck, opData, opErr, opFin1, OpFin2, opData, opWndSize, opRewind)
		self.mpx.Queue(self.Op(opAck, nil))
		return
	case opAck:
		self.status = self.status | stAck
		self.state.Broadcast()
		self.sLock.Unlock()
		self.mpx.Queue(self.Op(opSynAck, nil))
		return
	case opSynAck:
		self.status = self.status | stSynAck
	case opFin1:
		self.status = self.status | stFin1
	case opErr:
		self.status = self.status | stErr
	case opWndSize:
		self.wndSize = binary.BigEndian.Uint32(op.Data.Bytes)
	case opRewind:
		self.wBuf -= binary.BigEndian.Uint32(op.Data.Bytes)
	case opData:
		if len(op.Data.Bytes) > 0 {
			var buf = skykiss.BytesNew(len(op.Data.Bytes))
			copy(buf.Bytes, op.Data.Bytes)
			self.rBuf = append(self.rBuf, buf)
		}
	case OpFin2:
		var ioDeadline = time.Now().Add(TCP_CLOSE_WND)
		go func() {
			self.sLock.Lock()
			for time.Now().Before(ioDeadline) && len(self.rBuf) > 0 {
				skykiss.WaitTimeout(&self.state, ioDeadline.Sub(time.Now()))
			}
			self.status = self.status | stFin2
			self.state.Broadcast()
			self.sLock.Unlock()
		}()
	default:
	}

	self.state.Broadcast()
	self.sLock.Unlock()
}

func (self *stream) Write(b []byte) (n int, err error) {
	self.Join(stAck)

	self.sLock.Lock()
	if self.status&stFin1 > 0 {
		self.sLock.Unlock()
		return 0, io.EOF
	}
stateLoop:
	for {
		switch {
		case !self.wDeadLine.IsZero() && self.wDeadLine.Before(time.Now()):
			err = ErrTimeout
			break stateLoop
		case len(b) == 0:
			break stateLoop
		case self.wBuf < self.wndSize:
			var bLeft = int(self.wndSize - self.wBuf)
			if bLeft > len(b) {
				bLeft = len(b)
			}
			var data = b[0:bLeft]
			self.wBuf += uint32(len(data))
			n += len(data)
			b = b[len(data):]
			self.sLock.Unlock()
			var op = self.Op(opData, data)
			self.mpx.Send(op)
			self.sLock.Lock()
		case self.status&stFin1 > 0:
			err = io.EOF
			break stateLoop
		default:
			self.state.Wait()
		}
	}
	self.sLock.Unlock()
	return
}

func (self *stream) Read(b []byte) (n int, err error) {
	self.sLock.Lock()
	if self.status&stFin2 > 0 {
		self.sLock.Unlock()
		return 0, io.EOF
	}
	var buf skykiss.BytesPackage
stateLoop:
	for {
		switch {
		case !self.rDeadLine.IsZero() && self.rDeadLine.Before(time.Now()):
			err = ErrTimeout
			break stateLoop
		case len(b) == 0:
			break stateLoop
		case len(self.rBuf) > 0:
			buf = self.rBuf[0]
			var rBufN = copy(b, buf.Bytes)
			n += rBufN
			b = b[rBufN:]
			if rBufN == len(buf.Bytes) {
				var nl = copy(self.rBuf[0:], self.rBuf[1:])
				self.rBuf = self.rBuf[0:nl]
				skykiss.BytesRelease(buf)
			} else {
				self.rBuf[0].Bytes = self.rBuf[0].Bytes[rBufN:]
			}
			continue
		case len(self.rBuf) == 0 && n > 0:
			break stateLoop
		case self.status&stFin2 > 0:
			err = io.EOF
			break stateLoop
		default:
			self.state.Wait()
		}
	}
	self.state.Broadcast()
	self.sLock.Unlock()
	if n > 0 {
		var rwnd [4]byte
		binary.BigEndian.PutUint32(rwnd[:], uint32(n))
		self.mpx.Queue(self.Op(opRewind, rwnd[:]))
	}
	return
}

func (self *stream) Close() error {
	time.AfterFunc(time.Second*10, func() {
		self.recv(self.Op(opForceClosed, nil), nil)
		self.state.Broadcast()
		self.connHndl.Close()
	})
	self.recv(self.Op(opFin1, nil), nil)
	self.mpx.Queue(self.Op(OpFin2, nil))
	self.state.Broadcast()
	return nil
}

func (self *stream) SetDeadline(t time.Time) error {
	self.SetReadDeadline(t)
	self.SetWriteDeadline(t)
	return nil
}

func (self *stream) SetReadDeadline(t time.Time) error {
	self.rDeadLine = t
	time.AfterFunc(t.Sub(time.Now()), self.state.Broadcast)
	return nil
}

func (self *stream) SetWriteDeadline(t time.Time) error {
	self.wDeadLine = t
	time.AfterFunc(t.Sub(time.Now()), self.state.Broadcast)
	return nil
}

func (self *stream) LocalAddr() net.Addr {
	return &addr.Addr{self.network, self.local, self.lport}
}

func (self *stream) RemoteAddr() net.Addr {
	return &addr.Addr{self.network, self.remote, self.rport}
}

func (self *stream) Op(cmd protocol.Command, body []byte) protocol.Op {
	self.sLock.Lock()
	var op = protocol.Op{
		Cmd:    cmd,
		Local:  self.local,
		Remote: self.remote,
		LPort:  self.lport,
		RPort:  self.rport,
		Data:   skykiss.BytesPackage{Bytes: body},
	}
	self.sLock.Unlock()
	return op
}

func (self *stream) subscribe(cmds ...protocol.Command) {
	var filters = make([]transport.Filter, len(cmds))
	for idx, cmd := range cmds {
		filters[idx] = transport.Filter{
			Cmd:    cmd,
			Local:  self.remote,
			Remote: self.local,
			LPort:  self.rport,
			RPort:  self.lport,
		}
	}

	if self.connHndl != nil {
		self.connHndl.Close()
	}
	self.connHndl = self.mpx.Handle(self.recv, filters...)
}

func newSocket(network string, local uint64, lport uint32, mpx transport.Transport) *stream {
	var conn = &stream{
		network: network,
		local:   local,
		lport:   lport,
		mpx:     mpx,
		wndSize: WND_SIZE,
		status:  stNew,
	}
	conn.state.L = &conn.sLock
	return conn
}

func NewServerSocket(
	network string,
	local uint64, lport uint32,
	remote uint64, rport uint32,
	mpx transport.Transport,
) net.Conn {
	var conn = newSocket(network, local, lport, mpx)
	conn.remote = remote
	conn.rport = rport
	conn.connHndl = mpx.Handle(
		conn.recv,
	)
	conn.subscribe(opAck, opData, opErr, opFin1, OpFin2, opData, opWndSize, opRewind)
	conn.mpx.Queue(conn.Op(opSyn, nil))
	return conn
}

func NewClientSocket(
	network string,
	local uint64, lport uint32,
	mpx transport.Transport,
) net.Conn {
	var conn = newSocket(network, local, lport, mpx)
	conn.subscribe(opSyn)
	return conn
}
