// Package filestore implements a Blockstore which is able to read certain
// blocks of data directly from its original location in the filesystem.
//
// In a Filestore, object leaves are stored as FilestoreNodes. FilestoreNodes
// include a filesystem path and an offset, allowing a Blockstore dealing with
// such blocks to avoid storing the whole contents and reading them from their
// filesystem location instead.
package remotestore

import (
	"context"

	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/exchange/offline"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/boxo/ipld/unixfs/importer/trickle"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	dsq "github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/samber/oops"
)

var logger = logging.Logger("remotestore")

var _ blockstore.Blockstore = (*Remotestore)(nil)

// Remotestore implements a Blockstore by combining a standard Blockstore
// to store regular blocks and a special Blockstore called
// FileManager to store blocks which data exists in an external file.
type Remotestore struct {
	fm *RemoteManager
	bs blockstore.Blockstore
}

// RemoteManager returns the RemoteManager in Filestore.
func (f *Remotestore) RemoteManager() *RemoteManager {
	return f.fm
}

// MainBlockstore returns the standard Blockstore in the Filestore.
func (f *Remotestore) MainBlockstore() blockstore.Blockstore {
	return f.bs
}

// NewRemotestore creates one using the given Blockstore and FileManager.
func NewRemotestore(bs blockstore.Blockstore, fm *RemoteManager) *Remotestore {
	return &Remotestore{fm, bs}
}

// AllKeysChan returns a channel from which to read the keys stored in
// the blockstore. If the given context is cancelled the channel will be closed.
func (f *Remotestore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ctx, cancel := context.WithCancel(ctx)

	a, err := f.bs.AllKeysChan(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	out := make(chan cid.Cid, dsq.KeysOnlyBufSize)
	go func() {
		defer cancel()
		defer close(out)

		var done bool
		for !done {
			select {
			case c, ok := <-a:
				if !ok {
					done = true
					continue
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}

		// Can't do these at the same time because the abstractions around
		// leveldb make us query leveldb for both operations. We apparently
		// cant query leveldb concurrently
		b, err := f.fm.AllKeysChan(ctx)
		if err != nil {
			logger.Error("error querying filestore: ", err)
			return
		}

		done = false
		for !done {
			select {
			case c, ok := <-b:
				if !ok {
					done = true
					continue
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// DeleteBlock deletes the block with the given key from the
// blockstore. As expected, in the case of FileManager blocks, only the
// reference is deleted, not its contents. It may return
// ErrNotFound when the block is not stored.
func (f *Remotestore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	err1 := f.bs.DeleteBlock(ctx, c)
	if err1 != nil && !ipld.IsNotFound(err1) {
		return err1
	}

	err2 := f.fm.DeleteBlock(ctx, c)

	// if we successfully removed something from the blockstore, but the
	// filestore didnt have it, return success
	if !ipld.IsNotFound(err2) {
		return err2
	}

	if ipld.IsNotFound(err1) {
		return err1
	}

	return nil
}

// Get retrieves the block with the given Cid. It may return
// ErrNotFound when the block is not stored.
func (f *Remotestore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := f.bs.Get(ctx, c)
	if ipld.IsNotFound(err) {
		block, err := f.fm.Get(ctx, c)
		if err == nil {
			// cache remote block in local blockstore
			_ = f.bs.Put(ctx, block)
		}
		return block, err
	}
	return blk, err
}

// GetSize returns the size of the requested block. It may return ErrNotFound
// when the block is not stored.
func (f *Remotestore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	size, err := f.bs.GetSize(ctx, c)
	if err != nil {
		if ipld.IsNotFound(err) {
			return f.fm.GetSize(ctx, c)
		}
		return -1, err
	}
	return size, nil
}

// Has returns true if the block with the given Cid is
// stored in the Filestore.
func (f *Remotestore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	has, err := f.bs.Has(ctx, c)
	if err != nil {
		return false, err
	}

	if has {
		return true, nil
	}

	return f.fm.Has(ctx, c)
}

// Put stores a block in the Filestore. For blocks of
// underlying type FilestoreNode, the operation is
// delegated to the FileManager, while the rest of blocks
// are handled by the regular blockstore.
func (f *Remotestore) Put(ctx context.Context, b blocks.Block) error {
	has, err := f.Has(ctx, b.Cid())
	if err != nil {
		return err
	}

	if has {
		return nil
	}

	switch b := b.(type) {
	case *posinfo.FilestoreNode:
		return f.fm.Put(ctx, b)
	default:
		// skip bitswap raw nodes
		if !IsRawNodeCid(b.Cid()) {
			return f.bs.Put(ctx, b)
		}
		return nil
	}
}

// PutMany is like Put(), but takes a slice of blocks, allowing
// the underlying blockstore to perform batch transactions.
func (f *Remotestore) PutMany(ctx context.Context, bs []blocks.Block) error {
	var normals []blocks.Block
	var fstores []*posinfo.FilestoreNode

	for _, b := range bs {
		has, err := f.Has(ctx, b.Cid())
		if err != nil {
			return err
		}

		if has {
			continue
		}

		switch b := b.(type) {
		case *posinfo.FilestoreNode:
			fstores = append(fstores, b)
		default:
			normals = append(normals, b)
		}
	}

	if len(normals) > 0 {
		err := f.bs.PutMany(ctx, normals)
		if err != nil {
			return err
		}
	}

	if len(fstores) > 0 {
		err := f.fm.PutMany(ctx, fstores)
		if err != nil {
			return err
		}
	}
	return nil
}

// HashOnRead calls blockstore.HashOnRead.
func (f *Remotestore) HashOnRead(enabled bool) {
	f.bs.HashOnRead(enabled)
}

type SyncIndexOptions struct {
	// default is `default`
	Chunker string

	// default is `helpers.DefaultLinksPerBlock`
	Maxlinks int

	// default is "balanced"
	Layout string
}

func (f *Remotestore) SyncIndex(ctx context.Context, key string, opts SyncIndexOptions) (ipld.Node, error) {
	source := f.fm.source
	rc, size, err := source.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	chf := &MockFileInfo{
		MockAbspath: key,
		Reader:      rc,
		MockFileStat: &MockFileStat{
			MockName: key,
			MockSize: int64(size),
		},
	}

	bsrv := blockservice.New(f, offline.Exchange(f))
	dsrv := merkledag.NewDAGService(bsrv)

	params := helpers.DagBuilderParams{
		Dagserv:    dsrv,
		Maxlinks:   opts.Maxlinks,
		CidBuilder: merkledag.V0CidPrefix(),
		NoCopy:     true,
		RawLeaves:  true,
	}
	if params.Maxlinks == 0 {
		params.Maxlinks = helpers.DefaultLinksPerBlock
	}

	chnk, err := chunk.FromString(chf, opts.Chunker)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create chunker from file")
	}

	dbh, err := params.New(chnk)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create new dag builder helper")
	}

	var n ipld.Node
	switch opts.Layout {
	case "trickle":
		n, err = trickle.Layout(dbh)
	case "balanced", "":
		n, err = balanced.Layout(dbh)
	default:
		return nil, oops.Wrapf(err, "unknown layout: %s", opts.Layout)
	}

	return n, oops.Wrapf(err, "failed to layout dag")
}
