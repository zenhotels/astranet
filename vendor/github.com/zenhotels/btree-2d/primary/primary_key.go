package primary

import "github.com/zenhotels/btree-2d/common"

type Key struct {
	Value common.Comparable
}

func (p Key) Less(p2 Key) bool {
	return p.Value.Less(p2.Value)
}
