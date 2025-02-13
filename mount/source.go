package mount

import (
	"context"
	"io"
	"slices"
	"strings"

	rs "github.com/Dreamacro/go-ds-remote"
	ds "github.com/ipfs/go-datastore"
)

var _ rs.RemoteSource = (*Source)(nil)

type Mount struct {
	Prefix    ds.Key
	Datastore rs.RemoteSource
}

type Source struct {
	mounts []Mount
}

func New(mounts []Mount) *Source {
	mounts = slices.Clone(mounts)
	slices.SortFunc(mounts, func(a, b Mount) int {
		return strings.Compare(b.Prefix.String(), a.Prefix.String())
	})
	return &Source{mounts: mounts}
}

func (s *Source) lookup(key ds.Key) (rs.RemoteSource, ds.Key, ds.Key) {
	for _, m := range s.mounts {
		if m.Prefix.IsAncestorOf(key) {
			s := strings.TrimPrefix(key.String(), m.Prefix.String())
			k := ds.NewKey(s)
			return m.Datastore, m.Prefix, k
		}
	}
	return nil, ds.NewKey("/"), key
}

func (s *Source) GetPart(ctx context.Context, key string, offset uint64, size uint64) (io.ReadCloser, error) {
	source, _, k := s.lookup(ds.NewKey(key))
	if source == nil {
		return nil, ds.ErrNotFound
	}
	return source.GetPart(ctx, k.String(), offset, size)
}

func (s *Source) Get(ctx context.Context, key string) (io.ReadCloser, uint64, error) {
	source, _, k := s.lookup(ds.NewKey(key))
	if source == nil {
		return nil, 0, ds.ErrNotFound
	}
	return source.Get(ctx, k.String())
}
