package s3

import (
	"context"
	"io"

	rs "github.com/Dreamacro/go-ds-remote"
	"github.com/minio/minio-go/v7"
	"github.com/samber/oops"
)

var _ rs.RemoteSource = (*Source)(nil)

type Source struct {
	client *minio.Client
	bucket string
}

type Option func(*Source)

func New(client *minio.Client, bucket string, opts ...Option) *Source {
	s := &Source{
		client: client,
		bucket: bucket,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *Source) GetPart(ctx context.Context, key string, offset uint64, size uint64) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(int64(offset), int64(offset+size)); err != nil {
		return nil, err
	}

	output, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (s *Source) Get(ctx context.Context, key string) (io.ReadCloser, uint64, error) {
	output, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, oops.Wrapf(err, "failed to get object %s", key)
	}

	stat, err := output.Stat()
	if err != nil {
		return nil, 0, oops.Wrapf(err, "failed to stat object %s", key)
	}

	return output, uint64(stat.Size), nil
}
