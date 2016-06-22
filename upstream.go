package astranet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

var ErrTimeout = errors.New("i/o timeout")
var Dailimeout = errors.New("dial timeout")

type mpxRemote struct {
	keepalive   time.Duration
	reader      *bufio.Reader
	writer      *bufio.Writer
	wdeadline   time.Time
	tasker      *time.Ticker
	deadlines   map[int][]*sync.Cond
	dLock       sync.Mutex
	lastDlCheck int
	lAddr       net.Addr
	rAddr       net.Addr
	lastIOReq   time.Time
	lastIODone  time.Time
	lastOP      time.Time

	wDataBuf  []BytesPackage
	wDataSize int
	wSysBuf   []BytesPackage
	wNew      sync.Cond
	wLock     sync.Mutex

	wClosed      bool
	wCloseIfIdle bool
}

func (upstream *mpxRemote) init() *mpxRemote {
	upstream.wNew.L = &upstream.wLock
	upstream.tasker = time.NewTicker(time.Millisecond * 100)
	upstream.deadlines = make(map[int][]*sync.Cond)
	upstream.lastDlCheck = upstream.nowStep()
	upstream.dLock.Lock()
	upstream.lastIOReq = time.Now()
	upstream.lastIODone = time.Now()
	upstream.lastOP = time.Now()

	if upstream.keepalive == 0 {
		upstream.keepalive = time.Second * 10
	}
	upstream.dLock.Unlock()
	go upstream.wakeUpLoop()
	return upstream
}

func (upstream *mpxRemote) String() string {
	return fmt.Sprintf("%p(%v)", upstream, upstream.rAddr)
}

func (upstream *mpxRemote) nowStep() int {
	return upstream.time2step(time.Now())
}

func (upstream *mpxRemote) time2step(t time.Time) int {
	return t.Nanosecond() / 1e8
}

func (upstream *mpxRemote) setDeadline(when time.Time, c *sync.Cond) {
	var now = upstream.time2step(when)
	upstream.dLock.Lock()
	upstream.deadlines[now] = append(upstream.deadlines[now], c)
	upstream.dLock.Unlock()
}

func (upstream *mpxRemote) wakeUpLoop() {
	for range upstream.tasker.C {
		upstream.dLock.Lock()
		if upstream.wClosed {
			upstream.tasker.Stop()
			upstream.dLock.Unlock()
			break
		}
		if time.Now().Sub(upstream.lastIODone) > time.Duration(float64(upstream.keepalive)*0.8) {
			if !upstream.wCloseIfIdle {
				go upstream.Send(Op{Cmd: OP_NO_OP})
			}
		}

		if upstream.lastIOReq.Sub(upstream.lastIODone) > upstream.keepalive {
			mpxStatLog.Println("lastIO Inactivity")
			upstream.closeForced()
		}

		if time.Now().Sub(upstream.lastOP) > upstream.keepalive*10 {
			if upstream.wCloseIfIdle {
				mpxStatLog.Println("NOOP Inactivity [CLOSE_IF_IDLE_OPT]", upstream)
			} else {
				mpxStatLog.Println("NOOP Inactivity", upstream)
			}
			upstream.closeForced()
		}

		for i := upstream.lastDlCheck; i < upstream.nowStep(); i++ {
			var checkDl = upstream.deadlines[i]
			for _, c := range checkDl {
				c.Broadcast()
			}
			delete(upstream.deadlines, i)
			upstream.lastDlCheck = i
		}
		var needWakeUp bool
		if !upstream.wdeadline.IsZero() && upstream.wdeadline.Before(time.Now()) {
			needWakeUp = true
			upstream.wdeadline = time.Time{}
		}
		upstream.dLock.Unlock()
		if needWakeUp {
			upstream.wNew.Broadcast()
		}
	}
}

func (upstream *mpxRemote) SendTimeout(op Op, t time.Duration) (err error) {
	upstream.dLock.Lock()
	upstream.lastIOReq = time.Now()
	upstream.dLock.Unlock()

	upstream.wLock.Lock()
	if t != 0 {
		upstream.wdeadline = time.Now().Add(t)
	}

	switch op.Cmd {
	case OP_NEW, OP_SYN, OP_ACK:
		upstream.wSysBuf = append(upstream.wSysBuf, op.Encode())
	default:
	loopDone:
		for {
			switch {
			case upstream.wClosed:
				err = io.EOF
				break loopDone
			case t > 0 && upstream.wdeadline.Before(time.Now()):
				err = ErrTimeout
				break loopDone
			default:
				if upstream.wDataSize <= WND_SIZE {
					var frame = op.Encode()
					upstream.wDataBuf = append(upstream.wDataBuf, frame)
					upstream.wDataSize += len(frame.Bytes)
					break loopDone
				}
			}
			upstream.wNew.Wait()
		}
	}
	upstream.wNew.Broadcast()
	upstream.wLock.Unlock()
	return
}

func (upstream *mpxRemote) IOLoopWriter() error {
	var w = upstream.writer
	var jobs []BytesPackage
	upstream.wLock.Lock()
	for !upstream.wClosed {
		if len(upstream.wSysBuf)+len(upstream.wDataBuf) == 0 {
			upstream.wNew.Wait()
			continue
		}
		jobs = jobs[0:0]
		jobs = append(jobs, upstream.wSysBuf...)
		jobs = append(jobs, upstream.wDataBuf...)
		upstream.wSysBuf = upstream.wSysBuf[0:0]
		upstream.wDataBuf = upstream.wDataBuf[0:0]
		upstream.wDataSize = 0

		upstream.wNew.Broadcast()
		upstream.wLock.Unlock()

		for _, job := range jobs {
			var _, wBodyErr = w.Write(job.Bytes)
			BytesRelease(job)
			if wBodyErr != nil {
				return wBodyErr
			}
		}
		if wErr := w.Flush(); wErr != nil {
			return wErr
		}
		upstream.dLock.Lock()
		upstream.lastIODone = time.Now()
		upstream.dLock.Unlock()
		upstream.wLock.Lock()
	}
	upstream.wLock.Unlock()
	return nil
}

func (upstream *mpxRemote) Send(op Op) (err error) {
	return upstream.SendTimeout(op, 0)
}

func (upstream *mpxRemote) Close() {
	upstream.wLock.Lock()
	upstream.wClosed = true
	upstream.wdeadline = time.Now().Add(time.Second)
	upstream.wNew.Broadcast()
	upstream.wLock.Unlock()
}

func (upstream *mpxRemote) CloseIfIdle() {
	upstream.dLock.Lock()
	upstream.wCloseIfIdle = true
	upstream.dLock.Unlock()
}

func (upstream *mpxRemote) closeForced() {
	upstream.wLock.Lock()
	upstream.wClosed = true
	upstream.wdeadline = time.Now()
	upstream.wNew.Broadcast()
	upstream.wLock.Unlock()
}

func (upstream *mpxRemote) IsClosed() bool {
	if upstream == nil {
		return true
	}
	upstream.wLock.Lock()
	var isClosed = upstream.wClosed
	upstream.wLock.Unlock()
	return isClosed
}

type trackedConn struct {
	host  uint64
	port  uint32
	rhost uint64
	rport uint32
}

type mpxRoute struct {
	remote   uint64
	distance int
}
