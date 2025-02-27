package file

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"

	rs "github.com/Dreamacro/go-ds-remote"
	blockstore "github.com/ipfs/boxo/blockstore"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
)

var bg = context.Background()

func newTestFilestore(t *testing.T) (string, *rs.Remotestore) {
	mds := ds.NewMapDatastore()

	testdir, err := os.MkdirTemp("", "filestore-test")
	if err != nil {
		t.Fatal(err)
	}
	fm := rs.NewRemoteManager(mds, New(testdir))

	bs := blockstore.NewBlockstore(mds)
	fstore := rs.NewRemotestore(bs, fm)
	return testdir, fstore
}

func makeFile(dir string, data []byte) (string, error) {
	f, err := os.CreateTemp(dir, "file")
	if err != nil {
		return "", err
	}

	_, err = f.Write(data)
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}

func TestBasicFilestore(t *testing.T) {
	dir, fs := newTestFilestore(t)

	buf := make([]byte, 1000)
	rand.Read(buf)

	fname, err := makeFile(dir, buf)
	if err != nil {
		t.Fatal(err)
	}

	var cids []cid.Cid
	for i := 0; i < 100; i++ {
		n := &posinfo.FilestoreNode{
			PosInfo: &posinfo.PosInfo{
				FullPath: fname,
				Offset:   uint64(i * 10),
			},
			Node: dag.NewRawNode(buf[i*10 : (i+1)*10]),
		}

		err := fs.Put(bg, n)
		if err != nil {
			t.Fatal(err)
		}
		cids = append(cids, n.Node.Cid())
	}

	for i, c := range cids {
		blk, err := fs.Get(bg, c)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(blk.RawData(), buf[i*10:(i+1)*10]) {
			t.Fatal("data didnt match on the way out")
		}
	}

	kch, err := fs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	out := make(map[string]struct{})
	for c := range kch {
		out[c.KeyString()] = struct{}{}
	}

	if len(out) != len(cids) {
		t.Fatal("mismatch in number of entries")
	}

	for _, c := range cids {
		if _, ok := out[c.KeyString()]; !ok {
			t.Fatal("missing cid: ", c)
		}
	}
}

func randomFileAdd(t *testing.T, fs *rs.Remotestore, dir string, size int) (string, []cid.Cid) {
	buf := make([]byte, size)
	rand.Read(buf)

	fname, err := makeFile(dir, buf)
	if err != nil {
		t.Fatal(err)
	}

	var out []cid.Cid
	for i := 0; i < size/10; i++ {
		n := &posinfo.FilestoreNode{
			PosInfo: &posinfo.PosInfo{
				FullPath: fname,
				Offset:   uint64(i * 10),
			},
			Node: dag.NewRawNode(buf[i*10 : (i+1)*10]),
		}
		err := fs.Put(bg, n)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, n.Cid())
	}

	return fname, out
}

func TestDeletes(t *testing.T) {
	dir, fs := newTestFilestore(t)
	_, cids := randomFileAdd(t, fs, dir, 100)
	todelete := cids[:4]
	for _, c := range todelete {
		err := fs.DeleteBlock(bg, c)
		if err != nil {
			t.Fatal(err)
		}
	}

	deleted := make(map[string]bool)
	for _, c := range todelete {
		_, err := fs.Get(bg, c)
		if !ipld.IsNotFound(err) {
			t.Fatal("expected blockstore not found error")
		}
		deleted[c.KeyString()] = true
	}

	keys, err := fs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	for c := range keys {
		if deleted[c.KeyString()] {
			t.Fatal("shouldnt have reference to this key anymore")
		}
	}
}
