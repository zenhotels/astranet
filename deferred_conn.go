package astranet

import (
	"net"
	"time"
)

type deferredConn struct {
	upstream net.Conn
	upErr    error
	init     chan struct{}
}

func makeDeferredConn(h func() (net.Conn, error)) *deferredConn {
	var self = &deferredConn{}
	self.init = make(chan struct{})
	go func() {
		self.upstream, self.upErr = h()
		close(self.init)
	}()
	return self
}

func (self *deferredConn) Read(b []byte) (n int, err error) {
	<-self.init
	if self.upErr != nil {
		return 0, self.upErr
	}
	return self.upstream.Read(b)
}

func (self *deferredConn) Write(b []byte) (n int, err error) {
	<-self.init
	if self.upErr != nil {
		return 0, self.upErr
	}
	return self.upstream.Write(b)
}

func (self *deferredConn) Close() error {
	<-self.init
	if self.upErr != nil {
		return self.upErr
	}
	return self.upstream.Close()
}

func (self *deferredConn) SetDeadline(t time.Time) error {
	<-self.init
	if self.upErr != nil {
		return self.upErr
	}
	return self.upstream.SetDeadline(t)
}

func (self *deferredConn) SetReadDeadline(t time.Time) error {
	<-self.init
	if self.upErr != nil {
		return self.upErr
	}
	return self.upstream.SetReadDeadline(t)
}

func (self *deferredConn) SetWriteDeadline(t time.Time) error {
	<-self.init
	if self.upErr != nil {
		return self.upErr
	}
	return self.upstream.SetWriteDeadline(t)
}

func (self *deferredConn) LocalAddr() net.Addr {
	<-self.init
	if self.upErr != nil {
		return nil
	}
	return self.upstream.LocalAddr()
}

func (self *deferredConn) RemoteAddr() net.Addr {
	<-self.init
	if self.upErr != nil {
		return nil
	}
	return self.upstream.RemoteAddr()
}
