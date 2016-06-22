package skykiss

import (
	"sync"
	"testing"
)

func BenchmarkStructSyncPool(b *testing.B) {
	p := &sync.Pool{
		New: func() interface{} {
			return struct{}{}
		},
	}
	for i := 0; i < b.N; i++ {
		p.Put(p.Get())
	}
}

func BenchmarkBoolSyncPool(b *testing.B) {
	p := &sync.Pool{
		New: func() interface{} {
			return false
		},
	}
	for i := 0; i < b.N; i++ {
		p.Put(p.Get())
	}
}

func BenchmarkIntSyncPool(b *testing.B) {
	p := &sync.Pool{
		New: func() interface{} {
			return 0
		},
	}
	for i := 0; i < b.N; i++ {
		p.Put(p.Get())
	}
}
