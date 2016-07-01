package protocol

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/zenhotels/astranet/addr"
	"github.com/zenhotels/astranet/skykiss"
)

type Command uint8

type Op struct {
	Cmd    Command              // 1 byte
	Local  uint64               // 9 byte
	Remote uint64               // 17 byte
	LPort  uint32               // 21 byte
	RPort  uint32               // 25 byte
	Data   skykiss.BytesPackage // 29 byte with header
}

func (op Op) Encode() skykiss.BytesPackage {
	var bEnc = binary.BigEndian
	var buf = skykiss.BytesNew(29 + len(op.Data.Bytes))

	buf.Bytes[0] = byte(op.Cmd)
	bEnc.PutUint64(buf.Bytes[1:9], op.Remote)
	bEnc.PutUint64(buf.Bytes[9:17], op.Local)
	bEnc.PutUint32(buf.Bytes[17:21], op.RPort)
	bEnc.PutUint32(buf.Bytes[21:25], op.LPort)
	bEnc.PutUint32(buf.Bytes[25:29], uint32(len(op.Data.Bytes)))
	copy(buf.Bytes[29:], op.Data.Bytes)
	return buf
}

func (op *Op) Decode(buf []byte) {
	var bEnc = binary.BigEndian

	op.Cmd = Command(buf[0])
	op.Remote = bEnc.Uint64(buf[1:9])
	op.Local = bEnc.Uint64(buf[9:17])
	op.RPort = bEnc.Uint32(buf[17:21])
	op.LPort = bEnc.Uint32(buf[21:25])

	var bLen = int(bEnc.Uint32(buf[25:29]))
	op.Data.Bytes = buf[29 : 29+bLen]
}

func (op Op) String() string {
	return fmt.Sprintf(
		"OP{%d %s} {%s:%d->%s:%d} (%d) bytes",
		op.Cmd,
		op.Cmd.String(),
		addr.Uint2Host(op.Local), op.LPort,
		addr.Uint2Host(op.Remote), op.RPort,
		len(op.Data.Bytes),
	)
}

func RegisterFrame(ucode uint8, name string, priority bool) Command {
	var code = Command(ucode)
	if _, found := frames[code]; found {
		log.Fatal("Frame ", code, " has already been registered")
	}
	frames[code] = frameInfo{name, priority}
	return code
}

type frameInfo struct {
	name     string
	priority bool
}

var frames = map[Command]frameInfo{}

func (c Command) String() string {
	return frames[c].name
}

func (c Command) Priority() bool {
	return frames[c].priority
}
