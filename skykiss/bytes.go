package skykiss

import "sync"

type BytePool struct {
	Size int
	Pool *sync.Pool
}

type BytesPackage struct {
	Bytes []byte
	bi    interface{}
}

func BytePoolX(x int) BytePool {
	var pool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, x)
		},
	}
	return BytePool{x, pool}
}

func init() {
	for i := 1 << 8; i < 2<<18; i = i * 2 {
		bytePool = append(bytePool, BytePoolX(i))
	}
}

func BytesNew(size int) BytesPackage {
	for _, bPool := range bytePool {
		if bPool.Size > size {
			var bI =  bPool.Pool.Get()
			return BytesPackage{bI.([]byte)[0:size], bI}
		}
	}
	return BytesPackage{make([]byte, size), nil}
}

func BytesRelease(b BytesPackage) {
	if b.bi == nil {
		return
	}
	var size = cap(b.Bytes)
	for _, bPool := range bytePool {
		if bPool.Size == size {
			bPool.Pool.Put(b.bi)
			return
		}
	}
}

var bytePool = []BytePool{}
