package addr

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
)

type Addr struct {
	Net   string
	VHost uint64
	VPort uint32
}

func (self *Addr) Network() string {
	return self.Net
}

func (self *Addr) String() string {
	return fmt.Sprintf("%s:%d", Uint2Host(self.VHost), self.VPort)
}

func Uint2Host(h uint64) string {
	if h == 0 {
		return "--anyone--"
	}
	var src [9]byte
	src[0] = 0xFF
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
	if len(dec) != 9 {
		return 0, ErrShortHost
	} else if dec[1] != 0xFF {
		return 0, ErrNoHost
	}
	return binary.BigEndian.Uint64(dec[1:]), nil
}

var ErrEmptyHost = errors.New("Empty Hostname")
var ErrShortHost = errors.New("Short Hostname")
var ErrNoHost = errors.New("Not a Hostname")
