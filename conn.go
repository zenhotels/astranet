package astranet

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/zenhotels/astranet/skykiss"
)

var mpxLog = log.New(ioutil.Discard, "mpx ", log.LstdFlags|log.Lshortfile)
var mpxStatLog = log.New(ioutil.Discard, "mpx ", log.LstdFlags|log.Lshortfile)

func init() {
	if os.Getenv("MPXDEBUG") == "1" {
		mpxLog = log.New(os.Stderr, "mpx ", log.LstdFlags|log.Lshortfile)
	}
	if os.Getenv("MPXINFO") == "1" {
		mpxStatLog = log.New(os.Stderr, "mpx ", log.LstdFlags|log.Lshortfile)
	}
}

type IOLoop struct {
	io.Reader
	io.Writer
}

const (
	WND_SIZE      = 32 * 1024
	TCP_CLOSE_WND = 10 * time.Second
)

type ConnState uint8

const (
	OP_NEW  ConnState = 1 << iota
	OP_SYN            = 1 << iota
	OP_ACK            = 1 << iota // srv connection established
	OP_FIN1           = 1 << iota
	OP_FIN2           = 1 << iota
	OP_ERR            = 1 << iota // connection aborted
)

const (
	OP_SYN_ACK      = OP_SYN | OP_ACK
	OP_CLOSED       = OP_FIN1 | OP_FIN2
	OP_FORCE_CLOSED = OP_ERR | OP_CLOSED | OP_SYN_ACK
)

const (
	OP_DATA     ConnState = 128
	OP_WND_SIZE           = 129
	OP_REWIND             = 130
)

const (
	OP_DISCOVER   ConnState = 196
	OP_FORGET               = 197
	OP_SERVICE              = 198
	OP_NO_SERVICE           = 199
	OP_NO_OP                = 200
	OP_JOIN_ME              = 201
	OP_RHOST                = 202
)

func (c ConnState) String() string {
	switch c {
	case OP_NEW:
		return "OP_NEW"
	case OP_SYN:
		return "OP_SYN"
	case OP_ACK:
		return "OP_ACK"
	case OP_FIN1:
		return "OP_FIN1"
	case OP_FIN2:
		return "OP_FIN2"
	case OP_ERR:
		return "OP_ERR"
	case OP_SYN_ACK:
		return "OP_SYN_ACK"
	case OP_CLOSED:
		return "OP_CLOSED"
	case OP_FORCE_CLOSED:
		return "OP_FORCE_CLOSED"
	case OP_DATA:
		return "OP_DATA"
	case OP_WND_SIZE:
		return "OP_WND_SIZE"
	case OP_REWIND:
		return "OP_REWIND"
	case OP_DISCOVER:
		return "OP_DISCOVER"
	case OP_FORGET:
		return "OP_FORGET"
	case OP_SERVICE:
		return "OP_SERVICE"
	case OP_NO_SERVICE:
		return "OP_NO_SERVICE"
	case OP_NO_OP:
		return "OP_NO_OP"
	case OP_JOIN_ME:
		return "OP_JOIN_ME"
	case OP_RHOST:
		return "OP_RHOST"
	default:
		return "OP_UNKNOWN"
	}
}

type Stream struct {
	network string

	local  uint64
	remote uint64
	lport  uint32
	rport  uint32

	canSend uint32
	status  ConnState
	state   sync.Cond
	sLock   sync.Mutex

	rBuf []BytesPackage

	wBuf    uint32
	wndSize uint32

	mpx       *mpxRemote
	rDeadLine time.Time
	wDeadLine time.Time
}

func (self *Stream) Join(op ConnState) (state ConnState) {
	var match = func(op ConnState) bool {
		return (self.status & op) == op
	}
	self.state.L.Lock()
state_loop:
	for {
		switch {
		case !self.rDeadLine.IsZero() && self.rDeadLine.Before(time.Now()):
			self.status = self.status | OP_FORCE_CLOSED
			break state_loop
		case match(op):
			break state_loop
		case match(OP_CLOSED):
			break state_loop
		default:
			self.state.Wait()
		}
	}
	state = self.status
	self.state.L.Unlock()
	return
}

func (self *Stream) init() *Stream {
	self.state.L = &self.sLock
	self.wndSize = WND_SIZE
	return self
}

func (self *Stream) recv(op Op) {
	self.sLock.Lock()
	self.status = self.status | op.Cmd
	self.state.Broadcast()

	switch self.status {
	case OP_NEW:
		go self.mpx.Send(self.Op(OP_SYN, nil))
	case OP_SYN:
		go self.mpx.Send(self.Op(OP_ACK, nil))
	case OP_ACK:
		go self.mpx.Send(self.Op(OP_SYN_ACK, nil))
	}
	self.sLock.Unlock()
}

func (self *Stream) data(op Op) {
	self.sLock.Lock()
	switch op.Cmd {
	case OP_WND_SIZE:
		self.wndSize = binary.BigEndian.Uint32(op.Data.Bytes)
	case OP_REWIND:
		self.wBuf -= binary.BigEndian.Uint32(op.Data.Bytes)
	case OP_DATA:
		if len(op.Data.Bytes) > 0 {
			var buf = BytesNew(len(op.Data.Bytes))
			copy(buf.Bytes, op.Data.Bytes)
			self.rBuf = append(self.rBuf, buf)
		}

	case OP_FIN2:
		go func() {
			var forceCloseDeadline = time.Now().Add(TCP_CLOSE_WND)
			self.sLock.Lock()
			for {
				if len(self.rBuf) > 0 && time.Now().Before(forceCloseDeadline) {
					skykiss.WaitTimeout(&self.state, TCP_CLOSE_WND)
					continue
				}
				self.status = self.status | OP_FIN2
				break
			}
			self.sLock.Unlock()
			self.state.Broadcast()
		}()
	}
	self.sLock.Unlock()
	self.state.Broadcast()
}

func (self *Stream) Write(b []byte) (n int, err error) {
	self.Join(OP_ACK)

	self.sLock.Lock()
	if self.status&OP_FIN1 > 0 {
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
			var op = self.Op(OP_DATA, data)
			self.sLock.Unlock()
			self.mpx.Send(op)
			self.sLock.Lock()
		case self.status&OP_FIN1 > 0:
			err = io.EOF
			break stateLoop
		default:
			self.state.Wait()
		}
	}
	self.sLock.Unlock()
	return
}

func (self *Stream) Read(b []byte) (n int, err error) {
	self.sLock.Lock()
	if self.status&OP_FIN2 > 0 {
		self.sLock.Unlock()
		return 0, io.EOF
	}
	var buf BytesPackage
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
				BytesRelease(buf)
			} else {
				self.rBuf[0].Bytes = self.rBuf[0].Bytes[rBufN:]
			}
			continue
		case len(self.rBuf) == 0 && n > 0:
			break stateLoop
		case self.status&OP_FIN2 > 0:
			err = io.EOF
			break stateLoop
		default:
			self.state.Wait()
		}
	}
	self.sLock.Unlock()
	self.state.Broadcast()
	if n > 0 {
		var rwnd [4]byte
		binary.BigEndian.PutUint32(rwnd[:], uint32(n))
		self.mpx.Send(self.Op(OP_REWIND, rwnd[:]))
	}
	return
}

func (self *Stream) Close() error {
	time.AfterFunc(time.Second*10, func() {
		self.recv(self.Op(OP_FORCE_CLOSED, nil))
		self.state.Broadcast()
	})
	self.recv(self.Op(OP_FIN1, nil))
	self.mpx.Send(self.Op(OP_FIN2, nil))
	self.state.Broadcast()
	return nil
}

func (self *Stream) SetDeadline(t time.Time) error {
	self.SetReadDeadline(t)
	self.SetWriteDeadline(t)
	return nil
}

func (self *Stream) SetReadDeadline(t time.Time) error {
	self.rDeadLine = t
	self.mpx.setDeadline(t, &self.state)
	return nil
}

func (self *Stream) SetWriteDeadline(t time.Time) error {
	self.wDeadLine = t
	self.mpx.setDeadline(t, &self.state)
	return nil
}

func (self *Stream) LocalAddr() net.Addr {
	return &Addr{self.network, self.local, self.lport}
}

func (self *Stream) RemoteAddr() net.Addr {
	return &Addr{self.network, self.remote, self.rport}
}

func (self *Stream) Op(state ConnState, body []byte) Op {
	return Op{
		Cmd:    state,
		Local:  self.local,
		Remote: self.remote,
		LPort:  self.lport,
		RPort:  self.rport,
		Data:   BytesPackage{Bytes: body},
	}
}
