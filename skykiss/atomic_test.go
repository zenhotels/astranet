package skykiss

import (
	"sync/atomic"
	"testing"
	"sync"
)

func BenchmarkAValue(b *testing.B) {
	p := atomic.Value{}
	p.Store(123)
	for i := 0; i < b.N; i++ {
		p.Store(p.Load())
	}
}

func BenchmarkAAdd(b *testing.B) {
	p := uint64(123)
	for i := 0; i < b.N; i++ {
		atomic.AddUint64(&p, 1)
	}
}
func BenchmarkACas(b *testing.B) {
	p := uint64(123)
	for i := 0; i < b.N; i++ {
		atomic.CompareAndSwapUint64(&p, 123, 123)
	}
}
func BenchmarkMutex(b *testing.B) {
	var p sync.Mutex
	var v = 123
	for i := 0; i < b.N; i++ {
		p.Lock()
		v++
		p.Unlock()
	}
}

