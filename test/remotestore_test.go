package test

import (
	"bytes"
	"context"
	"testing"

	remotestore "github.com/Dreamacro/go-ds-remote"
	"github.com/Dreamacro/go-ds-remote/s3"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type object struct {
	Key   string
	Value []byte
	Cid   cid.Cid
}

func newClient(addr string) (*minio.Client, error) {
	mc, err := minio.New(addr, &minio.Options{
		Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
	})
	if err != nil {
		return nil, err
	}
	return mc, nil
}

func createFiles(t *testing.T, addr string, objects []object) error {
	ctx := context.Background()

	mc, err := newClient(addr)
	require.NoError(t, err)

	for _, obj := range objects {
		_, err = mc.PutObject(ctx, bucket, obj.Key, bytes.NewBuffer(obj.Value), int64(len(obj.Value)), minio.PutObjectOptions{})
		require.NoError(t, err)
	}

	return nil
}

func TestRemoteStore_Basic(t *testing.T) {
	addr, cleanup, err := createMinio()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = cleanup()
	})

	objects := []object{
		{"foo", bytes.Repeat([]byte("A"), 1024), cid.MustParse("bafkreidkw4xoxhtxwb2ubcl6bsgw2i7mr3xq7db2i7q3h5hjgrb5su3l5u")},
	}
	err = createFiles(t, addr, objects)
	require.NoError(t, err)

	mc, err := newClient(addr)
	require.NoError(t, err)

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
	bs := blockstore.NewBlockstore(datastore)
	s3s := s3.New(mc, bucket, "")
	rm := remotestore.NewRemoteManager(datastore, s3s)
	rs := remotestore.NewRemotestore(bs, rm)

	ctx := context.Background()

	for _, obj := range objects {
		node, err := s3s.SyncFile(ctx, s3.SyncFileOptions{
			Blockstore: rs,
			Key:        obj.Key,
			Path:       obj.Key,
		})
		assert.NoError(t, err)
		assert.Equalf(t, obj.Cid, node.Cid(), "cid mismatch")

		block, err := rs.Get(ctx, obj.Cid)
		assert.NoError(t, err)
		assert.Equal(t, obj.Value, block.RawData())
	}
}
