package remotestore

import (
	"io"
	"time"
)

var _ io.ReadCloser = (*innerProgress)(nil)

type innerProgress struct {
	key       string
	total     uint64
	bytesRead uint64
	err       error
	start     time.Time
	reader    io.ReadCloser
}

func (p *innerProgress) Read(buf []byte) (n int, err error) {
	n, err = p.reader.Read(buf)
	p.bytesRead += uint64(n)
	if err != nil && err != io.EOF {
		p.err = err
	}
	return
}

func (p *innerProgress) Close() error {
	return p.reader.Close()
}

type Progress struct {
	inner *innerProgress
}

func newProgress(key string, total uint64, reader io.ReadCloser) (*Progress, *innerProgress) {
	inner := &innerProgress{
		key:    key,
		total:  total,
		start:  time.Now(),
		reader: reader,
	}
	return &Progress{inner: inner}, inner
}

func (p *Progress) Total() uint64 {
	return p.inner.total
}

func (p *Progress) Err() error {
	return p.inner.err
}

func (p *Progress) StartTime() time.Time {
	return p.inner.start
}

func (p *Progress) Key() string {
	return p.inner.key
}

func (p *Progress) BytesRead() uint64 {
	return p.inner.bytesRead
}
