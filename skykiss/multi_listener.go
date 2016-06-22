package skykiss

import (
	"net"
	"sync"
)

type chWrap struct {
	conn net.Conn
	err  error
}

type multiListener struct {
	closer chan struct{}
	connCh chan chWrap
	closeCtl sync.Once
	addr net.Addr
}

func MultiListener(ll ...net.Listener) net.Listener {
	var self = &multiListener{}
	self.closer = make(chan struct{})
	self.connCh = make(chan chWrap)
	self.addr = ll[0].Addr()

	for _, l := range ll {
		go func(l net.Listener) {
		done:
			for {
				select {
				case <-self.closer:
					break done
				default:
				}
				var conn, connErr = l.Accept()
				self.connCh <- chWrap{conn, connErr}
			}
			l.Close()
		}(l)
	}
	return self
}

func (self *multiListener) Accept() (net.Conn, error) {
	var chW = <-self.connCh
	return chW.conn, chW.err
}

func (self *multiListener) Addr() net.Addr {
	return self.Addr()
}

func (self *multiListener) Close() error {
	self.closeCtl.Do(func() {
		close(self.closer)
	})
	return nil
}
