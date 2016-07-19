package secondary

import "github.com/zenhotels/btree-2d/common"

type Key struct {
	Value common.FinalizableComparable
}

func (s Key) Less(s2 Key) bool {
	return s.Value.Less(s2.Value)
}

func (s Key) Finalize() {
	s.Value.Finalize()
}
