package remotestore

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	pb "github.com/ipfs/boxo/filestore/pb"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
	proto "google.golang.org/protobuf/proto"
)

// RemotestorePrefix identifies the key prefix for FileManager blocks.
var RemotestorePrefix = ds.NewKey("remotestore")

// RemoteManager is a blockstore implementation which stores special
// blocks FilestoreNode type. These nodes only contain a reference
// to the actual location of the block data in the filesystem
// (a path and an offset).
type RemoteManager struct {
	ds     ds.Batching
	source RemoteSource
}

// CorruptReferenceError implements the error interface.
// It is used to indicate that the block contents pointed
// by the referencing blocks cannot be retrieved (i.e. the
// file is not found, or the data changed as it was being read).
type CorruptReferenceError struct {
	Code Status
	Err  error
}

// Error() returns the error message in the CorruptReferenceError
// as a string.
func (c CorruptReferenceError) Error() string {
	return c.Err.Error()
}

// NewRemoteManager initializes a new file manager with the given
// datastore and root. All FilestoreNodes paths are relative to the
// root path given here, which is prepended for any operations.
func NewRemoteManager(ds ds.Batching, source RemoteSource) *RemoteManager {
	return &RemoteManager{
		ds:     dsns.Wrap(ds, RemotestorePrefix),
		source: source,
	}
}

// AllKeysChan returns a channel from which to read the keys stored in
// the FileManager. If the given context is cancelled the channel will be
// closed.
//
// All CIDs returned are of type Raw.
func (f *RemoteManager) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	q := dsq.Query{KeysOnly: true}

	res, err := f.ds.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	out := make(chan cid.Cid, dsq.KeysOnlyBufSize)
	go func() {
		defer close(out)
		for {
			v, ok := res.NextSync()
			if !ok {
				return
			}

			k := ds.RawKey(v.Key)
			mhash, err := dshelp.DsKeyToMultihash(k)
			if err != nil {
				logger.Errorf("decoding cid from filestore: %s", err)
				continue
			}

			select {
			case out <- cid.NewCidV1(cid.Raw, mhash):
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// DeleteBlock deletes the reference-block from the underlying
// datastore. It does not touch the referenced data.
func (f *RemoteManager) DeleteBlock(ctx context.Context, c cid.Cid) error {
	err := f.ds.Delete(ctx, dshelp.MultihashToDsKey(c.Hash()))
	if err == ds.ErrNotFound {
		return ipld.ErrNotFound{Cid: c}
	}
	return err
}

// Get reads a block from the datastore. Reading a block
// is done in two steps: the first step retrieves the reference
// block from the datastore. The second step uses the stored
// path and offsets to read the raw block data directly from disk.
func (f *RemoteManager) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	dobj, err := f.getDataObj(ctx, c.Hash())
	if err != nil {
		return nil, err
	}
	out, err := f.readDataObj(ctx, c.Hash(), dobj)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(out, c)
}

// GetSize gets the size of the block from the datastore.
//
// This method may successfully return the size even if returning the block
// would fail because the associated file is no longer available.
func (f *RemoteManager) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	dobj, err := f.getDataObj(ctx, c.Hash())
	if err != nil {
		return -1, err
	}
	return int(dobj.GetSize()), nil
}

func (f *RemoteManager) readDataObj(ctx context.Context, m mh.Multihash, d *pb.DataObj) ([]byte, error) {
	fullpath := filepath.FromSlash(d.GetFilePath())

	reader, err := f.source.GetPart(ctx, fullpath, d.GetOffset(), d.GetSize())
	if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}
	defer reader.Close()

	outbuf := make([]byte, d.GetSize())
	_, err = io.ReadFull(reader, outbuf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, &CorruptReferenceError{StatusFileChanged, err}
	} else if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}

	// Work with CIDs for this, as they are a nice wrapper and things
	// will not break if multihashes underlying types change.
	origCid := cid.NewCidV1(cid.Raw, m)
	outcid, err := origCid.Prefix().Sum(outbuf)
	if err != nil {
		return nil, err
	}

	if !origCid.Equals(outcid) {
		return nil, &CorruptReferenceError{
			StatusFileChanged,
			fmt.Errorf("data in file did not match. %s offset %d", d.GetFilePath(), d.GetOffset()),
		}
	}

	return outbuf, nil
}

func (f *RemoteManager) getDataObj(ctx context.Context, m mh.Multihash) (*pb.DataObj, error) {
	o, err := f.ds.Get(ctx, dshelp.MultihashToDsKey(m))
	switch err {
	case ds.ErrNotFound:
		return nil, ipld.ErrNotFound{Cid: cid.NewCidV1(cid.Raw, m)}
	case nil:
		//
	default:
		return nil, err
	}

	return unmarshalDataObj(o)
}

func unmarshalDataObj(data []byte) (*pb.DataObj, error) {
	var dobj pb.DataObj
	if err := proto.Unmarshal(data, &dobj); err != nil {
		return nil, err
	}

	return &dobj, nil
}

// Has returns if the FileManager is storing a block reference. It does not
// validate the data, nor checks if the reference is valid.
func (f *RemoteManager) Has(ctx context.Context, c cid.Cid) (bool, error) {
	// NOTE: interesting thing to consider. Has doesnt validate the data.
	// So the data on disk could be invalid, and we could think we have it.
	dsk := dshelp.MultihashToDsKey(c.Hash())
	return f.ds.Has(ctx, dsk)
}

type putter interface {
	Put(context.Context, ds.Key, []byte) error
}

// Put adds a new reference block to the FileManager. It does not check
// that the reference is valid.
func (f *RemoteManager) Put(ctx context.Context, b *posinfo.FilestoreNode) error {
	return f.putTo(ctx, b, f.ds)
}

func (f *RemoteManager) putTo(ctx context.Context, b *posinfo.FilestoreNode, to putter) error {
	var dobj pb.DataObj

	filePath := filepath.ToSlash(b.PosInfo.FullPath)
	dobj.FilePath = &filePath
	dobj.Offset = &b.PosInfo.Offset
	size := uint64(len(b.RawData()))
	dobj.Size = &size

	data, err := proto.Marshal(&dobj)
	if err != nil {
		return err
	}

	return to.Put(ctx, dshelp.MultihashToDsKey(b.Cid().Hash()), data)
}

// PutMany is like Put() but takes a slice of blocks instead,
// allowing it to create a batch transaction.
func (f *RemoteManager) PutMany(ctx context.Context, bs []*posinfo.FilestoreNode) error {
	batch, err := f.ds.Batch(ctx)
	if err != nil {
		return err
	}

	for _, b := range bs {
		if err := f.putTo(ctx, b, batch); err != nil {
			return err
		}
	}

	return batch.Commit(ctx)
}
