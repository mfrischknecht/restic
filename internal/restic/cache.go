package restic

import (
	"context"
	"io"
)

// Cache manages a local cache.
type Cache interface {
	Backend

	// Wrap returns a backend with a cache.
	Wrap(CachedBackend) CachedBackend

	// Clear removes all files of type t from the cache that are not contained in the set.
	Clear(FileType, IDSet) error

	BaseDir() string

	Has(context.Context, Handle) bool
}

// Cache manages a local cache.
type CachedBackend interface {
	Backend

	// Clear removes all files of type t from the cache that are not contained in the set.
	ClearCache(FileType, IDSet) error

	SaveDirect(ctx context.Context, h Handle, rd RewindReader) error
	LoadDirect(ctx context.Context, h Handle, length int, offset int64, fn func(rd io.Reader) error) error
	IsCached(ctx context.Context, h Handle) bool
	OrigBackend() Backend
}
