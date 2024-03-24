package s3

import (
	"context"
	"io"
	"path"

	rs "github.com/Dreamacro/go-ds-remote"
	"github.com/ipfs/boxo/blockservice"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/boxo/ipld/unixfs/importer/trickle"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/minio/minio-go/v7"
	"github.com/samber/oops"
)

var _ rs.RemoteSource = (*Source)(nil)

type Source struct {
	client *minio.Client
	bucket string
	prefix string
}

func New(client *minio.Client, bucket, prefix string) *Source {
	return &Source{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}
}

func (s *Source) s3Path(key string) string {
	return path.Join(s.prefix, key)
}

func (s *Source) Get(ctx context.Context, key string, offset uint64, size uint64) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(int64(offset), int64(offset+size)); err != nil {
		return nil, err
	}

	output, err := s.client.GetObject(ctx, s.bucket, s.s3Path(key), opts)
	if err != nil {
		return nil, err
	}

	return output, nil
}

type SyncFileOptions struct {
	// required
	Blockstore *rs.Remotestore
	Key        string
	Path       string

	// default is `balanced`
	Chunker string

	// default is `helpers.DefaultLinksPerBlock`
	Maxlinks int

	// default is "balanced"
	Layout string
}

func (s *Source) SyncFile(ctx context.Context, opts SyncFileOptions) (ipld.Node, error) {
	output, err := s.client.GetObject(ctx, s.bucket, s.s3Path(opts.Path), minio.GetObjectOptions{})
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get object %s", opts.Key)
	}
	defer output.Close()

	stat, err := output.Stat()
	if err != nil {
		return nil, oops.Wrapf(err, "failed to stat object %s", opts.Key)
	}

	chf := &rs.MockFileInfo{
		MockAbspath: opts.Key,
		ReadCloser:  output,
		MockFileStat: &rs.MockFileStat{
			MockName: opts.Key,
			MockSize: stat.Size,
		},
	}

	bsrv := blockservice.New(opts.Blockstore, offline.Exchange(opts.Blockstore))
	dsrv := merkledag.NewDAGService(bsrv)

	params := helpers.DagBuilderParams{
		Dagserv:    dsrv,
		Maxlinks:   opts.Maxlinks,
		CidBuilder: merkledag.V1CidPrefix(),
		NoCopy:     true,
		RawLeaves:  true,
	}
	if params.Maxlinks == 0 {
		params.Maxlinks = helpers.DefaultLinksPerBlock
	}

	chnk, err := chunker.FromString(chf, opts.Chunker)
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
