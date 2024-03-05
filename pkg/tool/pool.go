package tool

import (
	"bytes"
	"sync"
)

var byteBufferPool sync.Pool

func byteBuffer() *bytes.Buffer {
	return new(bytes.Buffer)
}

func initByteBufferPool() {
	byteBufferPool = sync.Pool{
		New: func() any {
			return byteBuffer()
		},
	}
}

func GetByteBuffer() *bytes.Buffer {
	if byteBufferPool.New == nil {
		initByteBufferPool()
	}

	p, ok := byteBufferPool.Get().(*bytes.Buffer)
	if !ok {
		return byteBuffer()
	}
	return p
}

func PutByteBuffer(p *bytes.Buffer) {
	if byteBufferPool.New == nil {
		initByteBufferPool()
	}

	byteBufferPool.Put(p)
}
func ClearByteBuffer(p *bytes.Buffer) {
	p.Reset()
}
func ClearAndPutByteBuffer(p *bytes.Buffer) {
	ClearByteBuffer(p)
	PutByteBuffer(p)
}
