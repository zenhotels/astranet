package skykiss

func Chunker(sliceLen, chunkSize int, callable func(from, to int) error) (err error) {
	var sliceOffset = 0
	for sliceOffset+chunkSize < sliceLen {
		if err = callable(sliceOffset, sliceOffset+chunkSize); err != nil {
			return
		}
		sliceOffset += chunkSize
	}
	return callable(sliceOffset, sliceLen)
}
