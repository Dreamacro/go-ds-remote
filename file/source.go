package file

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	rs "github.com/Dreamacro/go-ds-remote"
)

var _ rs.RemoteSource = (*Source)(nil)

type limitReader struct {
	f *os.File
	n int64
}

func (l *limitReader) Read(p []byte) (n int, err error) {
	if l.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.n {
		p = p[0:l.n]
	}
	n, err = l.f.Read(p)
	l.n -= int64(n)
	return
}

func (l *limitReader) Close() error {
	return l.f.Close()
}

type Source struct {
	root string
}

func New(root string) *Source {
	return &Source{root: root}
}

func (s *Source) isSubPath(path string) bool {
	rel, err := filepath.Rel(s.root, path)
	if err != nil {
		return false
	}

	return !strings.Contains(rel, "..")
}

func (s *Source) Get(ctx context.Context, abspath string, offset uint64, size uint64) (io.ReadCloser, error) {
	if !s.isSubPath(abspath) {
		return nil, &rs.CorruptReferenceError{
			Code: rs.StatusOtherError,
			Err:  errors.New("file not in root path"),
		}
	}

	fi, err := os.Open(abspath)
	if os.IsNotExist(err) {
		return nil, &rs.CorruptReferenceError{
			Code: rs.StatusFileNotFound,
			Err:  err,
		}
	} else if err != nil {
		return nil, &rs.CorruptReferenceError{
			Code: rs.StatusFileError,
			Err:  err,
		}
	}

	_, err = fi.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, &rs.CorruptReferenceError{
			Code: rs.StatusFileError,
			Err:  err,
		}
	}

	return &limitReader{f: fi, n: int64(size)}, nil
}
