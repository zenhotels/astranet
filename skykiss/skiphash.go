package skykiss

import "hash"

type SkipHash struct {
	hash.Hash32
	bDone int
	bNext int
}

func (self *SkipHash) Write(p []byte) (n int, err error) {
	var skipF = 256
	var offset = self.bNext - self.bDone
	for i := offset; i < len(p); i += skipF {
		self.bNext += skipF
		self.Hash32.Write(p[i : i+1])
	}
	self.bDone += len(p)
	return len(p), nil
}
