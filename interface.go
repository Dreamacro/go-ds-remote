package remotestore

import (
	"context"
	"io"
)

type RemoteSource interface {
	Get(ctx context.Context, key string, offset uint64, size uint64) (io.ReadCloser, error)
}
