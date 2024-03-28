package test

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/minio/madmin-go"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minio "github.com/minio/minio/cmd"
	"github.com/samber/oops"
)

const (
	accessKeyID     = "minioadmin"
	secretAccessKey = "minioadmin"
	bucket1         = "test1"
	bucket2         = "test2"
)

func createMinio() (string, func() error, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", nil, oops.Wrapf(err, "while creating listener")
	}

	addr := l.Addr().String()
	err = l.Close()
	if err != nil {
		return "", nil, oops.Wrapf(err, "while closing listener")
	}

	madm, err := madmin.New(addr, accessKeyID, secretAccessKey, false)
	if err != nil {
		return "", nil, oops.Wrapf(err, "while creating madimin")
	}

	td, err := os.MkdirTemp(os.TempDir(), "")
	if err != nil {
		return "", nil, oops.Wrapf(err, "while creating temp dir")
	}

	go minio.Main([]string{"minio", "server", "--quiet", "--address", addr, td})

	// wait for server to start
	now := time.Now()
	for {
		if time.Since(now) > 5*time.Second {
			return "", nil, oops.Errorf("minio server did not start in time")
		}
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mc, err := mclient.New(addr, &mclient.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		return "", nil, oops.Wrapf(err, "while creating minio client")
	}

	err = mc.MakeBucket(context.Background(), bucket1, mclient.MakeBucketOptions{})
	if err != nil {
		return "", nil, oops.Wrapf(err, "while creating bucket")
	}

	err = mc.MakeBucket(context.Background(), bucket2, mclient.MakeBucketOptions{})
	if err != nil {
		return "", nil, oops.Wrapf(err, "while creating bucket")
	}

	return addr, func() error {
		err := madm.ServiceStop(context.Background())
		if err != nil {
			return oops.Wrapf(err, "while stopping service")
		}

		err = os.RemoveAll(td)
		if err != nil {
			return oops.Wrapf(err, "while deleting temp dir")
		}

		return nil
	}, nil
}
