package astranet

import (
	"encoding/binary"
	"hash"
	"hash/adler32"
	"io"
	"log"
	"math/rand"
	"sync"

	"time"

	"github.com/zenhotels/astranet/skykiss"
)

type RndReader struct {
	Seed  int64
	Limit int

	rnd      *rand.Rand
	initCtl  sync.Once
	chkSum   hash.Hash32
	wchkSum  hash.Hash32
	rndBlock [8192]byte
	rndPtr   int
	rSeqId   uint64

	read   int
	closed bool
	rLock  sync.Mutex
}

func (self *RndReader) init() {
	self.initCtl.Do(func() {
		if self.Seed == 0 {
			self.Seed = time.Now().UnixNano()
		}
		var src = rand.NewSource(self.Seed)
		self.rnd = rand.New(src)
		for i := 0; i < len(self.rndBlock)/8; i++ {
			binary.BigEndian.PutUint64(
				self.rndBlock[i*8:(i+1)*8],
				uint64(self.rnd.Int63()),
			)
		}
		self.chkSum = &skykiss.SkipHash{Hash32: adler32.New()}
		self.wchkSum = &skykiss.SkipHash{Hash32: adler32.New()}
	})
}

func (self *RndReader) Read(p []byte) (n int, err error) {
	self.init()
	self.rLock.Lock()
	if !self.closed {
		var lp = p[:]
		for len(lp) > 0 {
			var pLeft, rndLeft = len(lp), len(self.rndBlock) - self.rndPtr
			if rndLeft <= 0 {
				self.rndPtr = 0
				continue
			}
			var pMin = pLeft
			if rndLeft < pMin {
				pMin = rndLeft
			}
			n += copy(lp[0:pMin], self.rndBlock[self.rndPtr:self.rndPtr+pMin])
			self.rndPtr += n
			lp = p[n:]
		}
	}
	if self.Limit > 0 && self.read+n > self.Limit {
		n = self.Limit - self.read
		self.closed = true
	}
	self.chkSum.Write(p[0:n])
	self.read += n
	self.rLock.Unlock()
	if n == 0 {
		return 0, io.EOF
	}
	return
}

func (self *RndReader) Write(p []byte) (n int, err error) {
	self.init()
	self.rLock.Lock()
	n, err = self.wchkSum.Write(p)
	self.rLock.Unlock()
	return n, err
}

func (self *RndReader) Close() error {
	self.init()
	self.rLock.Lock()
	self.closed = true
	self.rLock.Unlock()
	return nil
}

func (self *RndReader) RChecksum() uint32 {
	self.Close()
	return self.chkSum.Sum32()
}

func (self *RndReader) WChecksum() uint32 {
	self.Close()
	return self.wchkSum.Sum32()
}

func printWrites(w io.Writer, chunkSize int, onlyOp bool) io.Writer {
	pipeOut, pipeIn := io.Pipe()
	wr := io.MultiWriter(w, pipeIn)
	go func() {
		buf := make([]byte, chunkSize)
		var err error
		var n int
		var dataWritten int
		for err != io.EOF {
			for i := range buf {
				buf[i] = 0
			}
			n, err = pipeOut.Read(buf)

			var op Op
			op.Decode(buf[:n])
			if op.Cmd == OP_DATA {
				dataWritten += len(op.Data.Bytes)
				continue
			} else if dataWritten > 0 {
				if !onlyOp {
					log.Printf("[WRITE DATA] %d bytes\n", dataWritten)
				}
				dataWritten = 0
			}

			if op.Cmd == OP_REWIND {
				dLen := binary.BigEndian.Uint32(op.Data.Bytes)
				log.Printf("[WRITE OP] %s [%d]", op.String(), dLen)
			} else {
				log.Printf("[WRITE OP] %s [%s]", op.String(), string(op.Data.Bytes))
			}
		}
	}()
	return wr
}
