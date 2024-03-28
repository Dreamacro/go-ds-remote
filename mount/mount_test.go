package mount_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/Dreamacro/go-ds-remote/mount"

	ds "github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
)

type mockSource struct{}

func (s *mockSource) GetPart(ctx context.Context, key string, offset uint64, size uint64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(key)), nil
}

func (s *mockSource) Get(ctx context.Context, key string) (io.ReadCloser, uint64, error) {
	return io.NopCloser(strings.NewReader(key)), uint64(len(key)), nil
}

func TestSource_Basic(t *testing.T) {
	ctx := context.Background()
	s := mount.New([]mount.Mount{
		{
			Prefix:    ds.NewKey("/"),
			Datastore: &mockSource{},
		},
		{
			Prefix:    ds.NewKey("/bar"),
			Datastore: &mockSource{},
		},
	})

	r, err := s.GetPart(ctx, "/foo", 0, 3)
	require.NoError(t, err)
	v, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "/foo", string(v))

	r, err = s.GetPart(ctx, "/bar/baz", 0, 3)
	require.NoError(t, err)
	v, err = io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "/baz", string(v))
}

func TestSource_HasBadNoMount(t *testing.T) {
	ctx := context.Background()
	s := mount.New([]mount.Mount{
		{
			Prefix:    ds.NewKey("/bar"),
			Datastore: &mockSource{},
		},
	})

	_, err := s.GetPart(ctx, "/foo", 0, 3)
	require.ErrorIs(t, err, ds.ErrNotFound)
}
