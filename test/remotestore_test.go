package test

import (
	"bytes"
	"context"
	"slices"
	"testing"

	remotestore "github.com/Dreamacro/go-ds-remote"
	"github.com/Dreamacro/go-ds-remote/s3"
	proto "github.com/gogo/protobuf/proto"
	"github.com/ipfs/boxo/blockstore"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/datastore/dshelp"
	pb "github.com/ipfs/boxo/filestore/pb"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
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

func unmarshalDataObj(data []byte) (*pb.DataObj, error) {
	var dobj pb.DataObj
	if err := proto.Unmarshal(data, &dobj); err != nil {
		return nil, err
	}

	return &dobj, nil
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

func TestRemoteStore(t *testing.T) {
	addr, cleanup, err := createMinio()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = cleanup()
	})

	t.Run("Basic", func(t *testing.T) { testRemoteStore_Basic(t, addr) })
	t.Run("Duplicate", func(t *testing.T) { testRemoteStore_Duplicate(t, addr) })
}

func testRemoteStore_Basic(t *testing.T, addr string) {
	objects := []object{
		{"foo", bytes.Repeat([]byte("A"), 1024), cid.MustParse("bafkreidkw4xoxhtxwb2ubcl6bsgw2i7mr3xq7db2i7q3h5hjgrb5su3l5u")},
	}
	err := createFiles(t, addr, objects)
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

		// cache not hit
		exist, err := bs.Has(ctx, obj.Cid)
		assert.NoError(t, err)
		assert.False(t, exist)

		block, err := rs.Get(ctx, obj.Cid)
		assert.NoError(t, err)
		assert.Equal(t, obj.Value, block.RawData())

		// cache hit
		exist, err = bs.Has(ctx, obj.Cid)
		assert.NoError(t, err)
		assert.True(t, exist)
	}
}

func testRemoteStore_Duplicate(t *testing.T, addr string) {
	objects := []object{
		{
			Key: "bar/baz",
			Value: slices.Concat(
				bytes.Repeat([]byte("A"), int(chunk.DefaultBlockSize)),
				bytes.Repeat([]byte("B"), 200),
			),
			Cid: cid.MustParse("QmU36W4ktZDYDGutmTaakCCuQVTFXhsdcQfNHPxhYW5EUM"),
		},
		{
			Key:   "foo",
			Value: bytes.Repeat([]byte("A"), int(chunk.DefaultBlockSize)),
			Cid:   cid.MustParse("bafkreiexul6fkqo4zhagxgnsvbgdjfq7udb26ig3uoli34xznjlmnpaaze"),
		},
	}
	err := createFiles(t, addr, objects)
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
		assert.Equalf(t, obj.Cid, node.Cid(), "cid mismatch %s", node.Cid().String())
	}

	{
		ds := dsns.Wrap(datastore, remotestore.RemotestorePrefix)
		value, err := ds.Get(ctx, dshelp.MultihashToDsKey(objects[1].Cid.Hash()))
		assert.NoError(t, err)
		pbobj, err := unmarshalDataObj(value)
		assert.NoError(t, err)
		assert.Equal(t, objects[0].Key, pbobj.FilePath)
	}
}
