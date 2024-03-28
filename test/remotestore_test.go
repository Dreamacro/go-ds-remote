package test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"testing"

	remotestore "github.com/Dreamacro/go-ds-remote"
	"github.com/Dreamacro/go-ds-remote/mount"
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

var minioAddr string

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

func createFiles(t *testing.T, addr, bucket string, objects []object) error {
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
	objects := []object{
		{"foo", bytes.Repeat([]byte("A"), 1024), cid.MustParse("bafkreidkw4xoxhtxwb2ubcl6bsgw2i7mr3xq7db2i7q3h5hjgrb5su3l5u")},
	}
	err := createFiles(t, minioAddr, bucket1, objects)
	require.NoError(t, err)

	mc, err := newClient(minioAddr)
	require.NoError(t, err)

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
	bs := blockstore.NewBlockstore(datastore)
	s3s := s3.New(mc, bucket1)
	rm := remotestore.NewRemoteManager(datastore, s3s)
	rs := remotestore.NewRemotestore(bs, rm)

	ctx := context.Background()

	for _, obj := range objects {
		node, err := rs.SyncIndex(ctx, obj.Key, remotestore.SyncIndexOptions{})
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

func TestRemoteStore_Duplicate(t *testing.T) {
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
	err := createFiles(t, minioAddr, bucket1, objects)
	require.NoError(t, err)

	mc, err := newClient(minioAddr)
	require.NoError(t, err)

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
	bs := blockstore.NewBlockstore(datastore)
	s3s := s3.New(mc, bucket1)
	rm := remotestore.NewRemoteManager(datastore, s3s)
	rs := remotestore.NewRemotestore(bs, rm)

	ctx := context.Background()

	for _, obj := range objects {
		node, err := rs.SyncIndex(ctx, obj.Key, remotestore.SyncIndexOptions{})
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

func TestRemoteStore_Mount(t *testing.T) {
	objects1 := []object{
		{
			Key: "foo",
			Value: slices.Concat(
				bytes.Repeat([]byte("A"), int(chunk.DefaultBlockSize)),
				bytes.Repeat([]byte("C"), 200),
			),
			Cid: cid.MustParse("QmZ1RzC21RH6gpAo6r45ZcqpDYQbRzAZdxqkZdkzkaCbb9"),
		},
	}

	err := createFiles(t, minioAddr, bucket1, objects1)
	require.NoError(t, err)

	objects2 := []object{
		{
			Key: "foo",
			Value: slices.Concat(
				bytes.Repeat([]byte("A"), int(chunk.DefaultBlockSize)),
				bytes.Repeat([]byte("B"), 200),
			),
			Cid: cid.MustParse("QmU36W4ktZDYDGutmTaakCCuQVTFXhsdcQfNHPxhYW5EUM"),
		},
	}

	err = createFiles(t, minioAddr, bucket2, objects2)
	require.NoError(t, err)

	mc, err := newClient(minioAddr)
	require.NoError(t, err)

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
	bs := blockstore.NewBlockstore(datastore)
	s3s := mount.New([]mount.Mount{
		{
			Prefix:    ds.NewKey("foo"),
			Datastore: s3.New(mc, bucket1),
		},
		{
			Prefix:    ds.NewKey("bar"),
			Datastore: s3.New(mc, bucket2),
		},
	})
	rm := remotestore.NewRemoteManager(datastore, s3s)
	rs := remotestore.NewRemotestore(bs, rm)

	ctx := context.Background()

	files := []struct {
		Key string
		Cid cid.Cid
	}{
		{
			Key: "foo/foo",
			Cid: objects1[0].Cid,
		},
		{
			Key: "bar/foo",
			Cid: objects2[0].Cid,
		},
	}

	for _, c := range files {
		node, err := rs.SyncIndex(ctx, c.Key, remotestore.SyncIndexOptions{})
		assert.NoError(t, err)
		assert.Equalf(t, c.Cid, node.Cid(), "cid mismatch %s", node.Cid().String())
	}

	cases := []struct {
		Cid  cid.Cid
		Path string
	}{
		{
			Cid:  cid.MustParse("bafkreiexul6fkqo4zhagxgnsvbgdjfq7udb26ig3uoli34xznjlmnpaaze"),
			Path: "foo/foo",
		},
	}

	for _, c := range cases {
		ds := dsns.Wrap(datastore, remotestore.RemotestorePrefix)
		value, err := ds.Get(ctx, dshelp.MultihashToDsKey(c.Cid.Hash()))
		assert.NoError(t, err)
		pbobj, err := unmarshalDataObj(value)
		assert.NoError(t, err)
		assert.Equal(t, c.Path, pbobj.FilePath)
	}
}

func TestMain(m *testing.M) {
	addr, cleanup, err := createMinio()
	if err != nil {
		fmt.Fprintf(os.Stderr, "while starting embedded server: %s", err)
		os.Exit(1)
	}
	minioAddr = addr

	exitCode := m.Run()
	if err := cleanup(); err != nil {
		fmt.Fprintf(os.Stderr, "while stopping embedded server: %s", err)
	}

	os.Exit(exitCode)
}
