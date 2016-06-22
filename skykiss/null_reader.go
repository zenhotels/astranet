package skykiss

type NullReader struct {
	Chunk int
}

func (self NullReader) Read(p []byte) (n int, err error) {
	if self.Chunk == 0 {
		return len(p), nil
	}
	return self.Chunk, nil
}
