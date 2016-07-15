package transport

import (
	"bufio"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"log"

	"github.com/zenhotels/astranet/glog"
	"github.com/zenhotels/astranet/protocol"
	"github.com/zenhotels/astranet/skykiss"
)

var ErrTimeout = errors.New("i/o timeout")

type transport struct {
	Router

	id        string
	keepalive time.Duration
	reader    *bufio.Reader
	writer    *bufio.Writer
	closer    io.Closer

	wdeadline   time.Time
	tasker      *time.Ticker
	deadlines   map[int][]*sync.Cond
	dLock       sync.Mutex
	lastDlCheck int
	rAddr       net.Addr
	lastIOReq   time.Time
	lastIODone  time.Time
	lastOP      time.Time

	wDataBuf  []skykiss.BytesPackage
	wDataSize int
	wSysBuf   []skykiss.BytesPackage
	wNew      sync.Cond
	wLock     sync.Mutex

	wClosed      bool
	wCloseIfIdle bool

	Log glog.Logger
}

func (self *transport) init() *transport {
	self.wNew.L = &self.wLock
	self.tasker = time.NewTicker(time.Millisecond * 100)
	self.deadlines = make(map[int][]*sync.Cond)
	self.lastDlCheck = self.nowStep()
	self.dLock.Lock()
	self.lastIOReq = time.Now()
	self.lastIODone = time.Now()
	self.lastOP = time.Now()
	self.id = fmt.Sprintf("up:%d", upstreamId.Next())

	if self.keepalive == 0 {
		self.keepalive = time.Second * 10
	}
	self.dLock.Unlock()

	go self.wakeUpLoop()
	go self.IOLoopReader()
	go self.IOLoopWriter()

	return self
}

func (self *transport) String() string {
	return fmt.Sprintf("%p(%v)", self, self.rAddr)
}

func (self *transport) RAddr() net.Addr {
	return self.rAddr
}

func (self *transport) nowStep() int {
	return self.time2step(time.Now())
}

func (self *transport) time2step(t time.Time) int {
	return t.Nanosecond() / 1e8
}

func (self *transport) setDeadline(when time.Time, c *sync.Cond) {
	var now = self.time2step(when)
	self.dLock.Lock()
	self.deadlines[now] = append(self.deadlines[now], c)
	self.dLock.Unlock()
}

func (self *transport) wakeUpLoop() {
	for range self.tasker.C {
		self.dLock.Lock()
		if self.wClosed {
			self.tasker.Stop()
			self.dLock.Unlock()
			break
		}
		if time.Now().Sub(self.lastIODone) > time.Duration(float64(self.keepalive)*0.5) {
			if !self.wCloseIfIdle {
				go self.Send(protocol.Op{Cmd: opNoOp})
			}
		}

		if self.lastIOReq.Sub(self.lastIODone) > self.keepalive {
			self.Log.VLog(20, func(l *log.Logger) {
				l.Println("lastIO Inactivity", self.id)
			})
			self.closeForced()
		}

		if time.Now().Sub(self.lastOP) > self.keepalive*10 {
			if self.wCloseIfIdle {
				self.Log.VLog(20, func(l *log.Logger) {
					l.Println("NOOP Inactivity [CLOSE_IF_IDLE_OPT]", self, self.id)
				})
			} else {
				self.Log.VLog(20, func(l *log.Logger) {
					l.Println("NOOP Inactivity", self, self.id)
				})
			}
			self.closeForced()
		}

		for i := self.lastDlCheck; i < self.nowStep(); i++ {
			var checkDl = self.deadlines[i]
			for _, c := range checkDl {
				c.Broadcast()
			}
			delete(self.deadlines, i)
			self.lastDlCheck = i
		}
		var needWakeUp bool
		if !self.wdeadline.IsZero() && self.wdeadline.Before(time.Now()) {
			needWakeUp = true
			self.wdeadline = time.Time{}
		}
		self.dLock.Unlock()
		if needWakeUp {
			self.wNew.Broadcast()
		}
	}
}

func (self *transport) SendTimeout(op protocol.Op, t time.Duration) (err error) {
	self.dLock.Lock()
	self.lastIOReq = time.Now()
	self.dLock.Unlock()

	self.Log.VLog(50, func(l *log.Logger) {
		l.Printf("%s SEND %s", self.id, op.String())
	})

	var deadline time.Time
	if t != 0 {
		deadline = time.Now().Add(t)
		time.AfterFunc(t, self.wNew.Broadcast)
	}

	self.wLock.Lock()
	switch {
	case op.Cmd.Priority():
		self.wSysBuf = append(self.wSysBuf, op.Encode())
	default:
	loopDone:
		for {
			switch {
			case self.wClosed:
				err = io.EOF
				break loopDone
			case t > 0 && deadline.Before(time.Now()):
				err = ErrTimeout
				break loopDone
			default:
				if self.wDataSize <= WND_SIZE {
					var frame = op.Encode()
					self.wDataBuf = append(self.wDataBuf, frame)
					self.wDataSize += len(frame.Bytes)
					break loopDone
				}
			}
			self.wNew.Wait()
		}
	}
	self.wNew.Broadcast()
	self.wLock.Unlock()
	return
}

func (self *transport) Queue(op protocol.Op) {
	self.Log.VLog(50, func(l *log.Logger) {
		l.Printf("%s QUEUE %s", self.id, op.String())
	})

	self.wLock.Lock()
	switch {
	case op.Cmd.Priority():
		self.wSysBuf = append(self.wSysBuf, op.Encode())
	default:
		var frame = op.Encode()
		self.wDataBuf = append(self.wDataBuf, frame)
		self.wDataSize += len(frame.Bytes)
	}
	self.wNew.Broadcast()
	self.wLock.Unlock()
	return
}

func (self *transport) IOLoopWriter() error {
	var w = self.writer
	var jobs []skykiss.BytesPackage
	self.wLock.Lock()
	for !self.wClosed {
		if len(self.wSysBuf)+len(self.wDataBuf) == 0 {
			self.wNew.Wait()
			continue
		}
		self.Log.VLog(70, func(l *log.Logger) {
			l.Printf("%s wDataSize is %d", self.id, self.wDataSize)
		})
		jobs = jobs[0:0]
		jobs = append(jobs, self.wSysBuf...)
		jobs = append(jobs, self.wDataBuf...)
		self.wSysBuf = self.wSysBuf[0:0]
		self.wDataBuf = self.wDataBuf[0:0]
		self.wDataSize = 0

		self.wNew.Broadcast()
		self.wLock.Unlock()

		for _, job := range jobs {
			var _, wBodyErr = w.Write(job.Bytes)
			self.Log.VLog(70, func(l *log.Logger) {
				l.Printf("%s IOLoopWriter %d bytes sent: %v", self.id, len(job.Bytes), wBodyErr)
			})
			skykiss.BytesRelease(job)
			if wBodyErr != nil {
				self.Log.VLog(10, func(l *log.Logger) {
					l.Println("ERR while writing to UPSTREAM", wBodyErr, self.id)
				})
				return wBodyErr
			}
		}
		if wErr := w.Flush(); wErr != nil {
			self.Log.VLog(10, func(l *log.Logger) {
				l.Println("ERR while flushing to UPSTREAM", wErr, self.id)
			})
			return wErr
		}
		self.Log.VLog(70, func(l *log.Logger) {
			l.Printf("%s IOLoopWriter flush done", self.id)
		})
		self.dLock.Lock()
		self.lastIODone = time.Now()
		self.dLock.Unlock()
		self.wLock.Lock()
	}
	self.wLock.Unlock()
	return nil
}

func (self *transport) Send(op protocol.Op) (err error) {
	return self.SendTimeout(op, 0)
}

func (self *transport) Close() {
	self.wLock.Lock()
	if !self.wClosed {
		self.Log.VLog(30, func(l *log.Logger) {
			l.Printf("%s CLOSED", self.id)
		})
	}
	self.wClosed = true
	self.wdeadline = time.Now().Add(time.Second)
	self.wNew.Broadcast()
	var closer = self.closer
	self.wLock.Unlock()
	if closer != nil {
		go closer.Close()
	}
}

func (self *transport) Drain() {
	self.dLock.Lock()
	self.wCloseIfIdle = true
	self.dLock.Unlock()
}

func (self *transport) closeForced() {
	self.wLock.Lock()
	self.wClosed = true
	self.wdeadline = time.Now()
	self.wNew.Broadcast()
	var closer = self.closer
	self.wLock.Unlock()
	if closer != nil {
		go closer.Close()
	}
}

func (self *transport) IsClosed() bool {
	if self == nil {
		return true
	}
	self.wLock.Lock()
	var isClosed = self.wClosed
	self.wLock.Unlock()
	return isClosed
}

func (self *transport) IOLoopReader() error {
	var bEnc = binary.BigEndian
	defer func() {
		self.Close()
	}()

	var opLock sync.RWMutex
	var opStart time.Time
	var opHeader protocol.Op
	var opDone time.Time

	var opLatencyCheck func()
	opLatencyCheck = func() {
		opLock.RLock()
		if opDone.Before(opStart) && time.Now().Sub(opStart) > 10*time.Second {
			self.Log.Panic(
				"FREEZE while reading from UPSTREAM", opHeader, self.id, time.Now().Sub(opStart),
			)
		}
		opLock.RUnlock()
		time.AfterFunc(time.Second, opLatencyCheck)
	}
	opLatencyCheck()
	for {
		opLock.Lock()
		opDone = time.Now()
		opLock.Unlock()

		var job protocol.Op
		var header, headerErr = self.reader.Peek(29)
		if headerErr == nil {
			_, headerErr = self.reader.Discard(29)
		}

		if headerErr != nil {
			self.Log.VLog(10, func(l *log.Logger) {
				l.Println("ERR while reading from UPSTREAM", headerErr, self.id)
			})
			return headerErr
		}

		job.Cmd = protocol.Command(header[0])
		job.Remote = bEnc.Uint64(header[1:9])
		job.Local = bEnc.Uint64(header[9:17])
		job.RPort = bEnc.Uint32(header[17:21])
		job.LPort = bEnc.Uint32(header[21:25])

		opLock.Lock()
		opStart = time.Now()
		opHeader = job
		opLock.Unlock()

		self.Log.VLog(50, func(l *log.Logger) {
			l.Printf("%s HEAD %s", self.id, job.String())
		})

		var bLen = int(bEnc.Uint32(header[25:29]))
		var dataErr error
		job.Data.Bytes, dataErr = self.reader.Peek(bLen)
		if dataErr == nil {
			_, dataErr = self.reader.Discard(bLen)
		}

		if dataErr != nil {
			self.Log.VLog(10, func(l *log.Logger) {
				l.Println("ERR while reading from UPSTREAM", dataErr, self.id)
			})
			return dataErr
		}

		self.Log.VLog(50, func(l *log.Logger) {
			l.Printf("%s RECV %s", self.id, job.String())
		})

		self.dLock.Lock()
		self.lastOP = time.Now()
		self.dLock.Unlock()

		switch job.Cmd {
		case opNoOp:
			continue
		}

		var cb = self.CheckFrame(job)
		if cb == nil {
			panic(fmt.Sprint("No handler found for frame", job))
		}
		cb(job, self)
	}
	return nil
}

func (self *transport) Join() {
	self.wLock.Lock()
	for !self.wClosed {
		self.wNew.Wait()
	}
	self.wLock.Unlock()
}

func Upstream(rw io.ReadWriter, l glog.Logger, hndl Callback, keepalive time.Duration) Transport {
	var tr = &transport{}
	tr.reader = bufio.NewReaderSize(rw, 64*1024)
	tr.writer = bufio.NewWriterSize(rw, 64*1024)
	tr.keepalive = keepalive
	tr.Log = l
	tr.closer, _ = rw.(io.Closer)

	if c, ok := rw.(net.Conn); ok {
		tr.rAddr = c.RemoteAddr()
	}

	tr.Router.Handle(hndl, Filter{})
	tr.init()
	return tr
}

type handler struct {
	cb        Callback
	el        map[Filter]*list.Element
	transport *Router
	closed    bool
}

func (self *handler) Close() {
	if !self.closed {
		for f, el := range self.el {
			self.transport.CloseHandle(f, el)
		}
		self.closed = true
	}
}

var upstreamId skykiss.AutoIncSequence
