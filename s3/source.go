package s3

import (
	"context"
	"fmt"
	"io"
	"path"

	rs "github.com/Dreamacro/go-ds-remote"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var _ rs.RemoteSource = (*Source)(nil)

type Source struct {
	s3     *s3.S3
	bucket string
	prefix string
}

func (s *Source) s3Path(key string) string {
	return path.Join(s.prefix, key)
}

func (s *Source) Get(ctx context.Context, key string, offset uint64, size uint64) (io.ReadCloser, error) {
	output, err := s.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.s3Path(key)),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+size)),
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}
