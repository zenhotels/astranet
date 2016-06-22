package astranet

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"errors"
)

type Addr struct {
	network string
	VHost   uint64
	VPort   uint32
}

func (self *Addr) Network() string {
	return self.network
}

func (self *Addr) String() string {
	return fmt.Sprintf("%s:%d", Uint2Host(self.VHost), self.VPort)
}

func Uint2Host(h uint64) string {
	var src [8]byte
	binary.BigEndian.PutUint64(src[:], h)
	return base64.RawURLEncoding.EncodeToString(src[:])
}

func Host2Uint(h string) (uint64, error) {
	if h == "" {
		return 0, ErrEmptyHost
	}
	var dec, decErr = base64.RawURLEncoding.DecodeString(h)
	if decErr != nil {
		return 0, decErr
	}
	if len(dec) != 8 {
		return 0, ErrShortHost
	}
	return binary.BigEndian.Uint64(dec[:]), nil
}

var ErrEmptyHost = errors.New("Empty Hostname")
var ErrShortHost = errors.New("Short Hostname")