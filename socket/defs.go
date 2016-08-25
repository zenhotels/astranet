package socket

import (
	"errors"

	"github.com/zenhotels/astranet/protocol"
)

type ConnState uint8

const (
	stNew  ConnState = 1 << iota
	stSyn  ConnState = 1 << iota
	stAck  ConnState = 1 << iota
	stFin1 ConnState = 1 << iota
	stFin2 ConnState = 1 << iota
	stErr  ConnState = 1 << iota

	stSynAck      ConnState = stSyn | stAck
	stClosed      ConnState = stFin1 | stFin2
	stForceClosed ConnState = stErr | stClosed | stSynAck
)

const (
	WND_SIZE = 65536
)

var (
	opSyn  = protocol.RegisterFrame(uint8(stSyn), "OP_SYN", true)
	opAck  = protocol.RegisterFrame(uint8(stAck), "OP_ACK", true)
	opFin1 = protocol.RegisterFrame(uint8(stFin1), "OP_FIN1", false)
	opFin2 = protocol.RegisterFrame(uint8(stFin2), "OP_FIN2", false)
	opErr  = protocol.RegisterFrame(uint8(stErr), "OP_ERR", false)

	opSynAck      = protocol.RegisterFrame(uint8(stSynAck), "OP_SYN_ACK", true)
	opForceClosed = protocol.RegisterFrame(uint8(stForceClosed), "OP_FORCE_CLOSED", false)

	opData    = protocol.RegisterFrame(128, "OP_DATA", false)
	opWndSize = protocol.RegisterFrame(129, "OP_WND_SIZE", false)
	opRewind  = protocol.RegisterFrame(130, "OP_REWIND", true)
)

var ErrTimeout = errors.New("i/o timeout")
