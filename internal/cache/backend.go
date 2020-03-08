package cache

import (
	"context"
	"io"
	"sync"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
)

// Backend wraps a restic.Backend and adds a cache.
type Backend struct {
	restic.CachedBackend
	*Cache

	// inProgress contains the handle for all files that are currently
	// downloaded. The channel in the value is closed as soon as the download
	// is finished.
	inProgressMutex sync.Mutex
	inProgress      map[restic.Handle]chan struct{}
}

// ensure cachedBackend implements restic.CachedBackend
var _ restic.CachedBackend = &Backend{}

func newBackend(be restic.CachedBackend, c *Cache) *Backend {
	return &Backend{
		CachedBackend: be,
		Cache:         c,
		inProgress:    make(map[restic.Handle]chan struct{}),
	}
}

// Remove deletes a file from the backend and the cache if it has been cached.
func (b *Backend) Remove(ctx context.Context, h restic.Handle) error {
	debug.Log("cache Remove(%v)", h)
	err := b.CachedBackend.Remove(ctx, h)
	if err != nil {
		return err
	}

	_ = b.Cache.Remove(ctx, h)
	return nil
}

// Save stores a new file in the backend and the cache.
func (b *Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	debug.Log("Save(%v): auto-store in the cache", h)

	// make sure the reader is at the start
	err := rd.Rewind()
	if err != nil {
		return err
	}

	// first, save in the backend
	err = b.CachedBackend.Save(ctx, h, rd)
	if err != nil {
		return err
	}

	// next, save in the cache
	err = rd.Rewind()
	if err != nil {
		return err
	}

	err = b.Cache.Save(ctx, h, rd)
	if err != nil {
		debug.Log("unable to save %v to cache: %v", h, err)
		_ = b.Cache.Remove(ctx, h)
		return nil
	}

	return nil
}

var autoCacheFiles = map[restic.FileType]bool{
	restic.IndexFile:    true,
	restic.SnapshotFile: true,
}

func (b *Backend) cacheFile(ctx context.Context, h restic.Handle) error {
	finish := make(chan struct{})

	b.inProgressMutex.Lock()
	other, alreadyDownloading := b.inProgress[h]
	if !alreadyDownloading {
		b.inProgress[h] = finish
	}
	b.inProgressMutex.Unlock()

	if alreadyDownloading {
		debug.Log("readahead %v is already performed by somebody else, delegating...", h)
		<-other
		debug.Log("download %v finished", h)
		return nil
	}

	// test again, maybe the file was cached in the meantime
	if !b.Cache.Has(ctx, h) {
		// nope, it's still not in the cache, pull it from the repo and save it
		tmpfile, err := fs.TempFile("", "restic-temp-cache-")
		if err != nil {
			return errors.Wrap(err, "fs.TempFile")
		}

		err = b.CachedBackend.Load(ctx, h, 0, 0, func(rd io.Reader) error {
			_, err := io.Copy(tmpfile, rd)
			if err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			// try to remove from the cache, ignore errors
			_ = b.Cache.Remove(ctx, h)
		}

		tmpRd, err := restic.NewFileReader(tmpfile)
		if err != nil {
			return err
		}

		tmpRd.Rewind()
		err = b.Cache.Save(ctx, h, tmpRd)
		if err != nil {
			return err
		}

	}

	// signal other waiting goroutines that the file may now be cached
	close(finish)

	// remove the finish channel from the map
	b.inProgressMutex.Lock()
	delete(b.inProgress, h)
	b.inProgressMutex.Unlock()

	return nil
}

// loadFromCacheOrDelegate will try to load the file from the cache, and fall
// back to the backend if that fails.
func (b *Backend) loadFromCacheOrDelegate(ctx context.Context, h restic.Handle, length int, offset int64, consumer func(rd io.Reader) error) error {
	err := b.Cache.Load(ctx, h, length, offset, consumer)
	if err != nil {
		debug.Log("error caching %v: %v, falling back to backend", h, err)
		return b.CachedBackend.Load(ctx, h, length, offset, consumer)
	}
	return nil
}

// Load loads a file from the cache or the backend.
func (b *Backend) Load(ctx context.Context, h restic.Handle, length int, offset int64, consumer func(rd io.Reader) error) error {
	b.inProgressMutex.Lock()
	waitForFinish, inProgress := b.inProgress[h]
	b.inProgressMutex.Unlock()

	if inProgress {
		debug.Log("downloading %v is already in progress, waiting for finish", h)
		<-waitForFinish
		debug.Log("downloading %v finished", h)
	}

	if b.Cache.Has(ctx, h) {
		debug.Log("Load(%v, %v, %v) from cache", h, length, offset)
		return b.Cache.Load(ctx, h, length, offset, consumer)
	}

	debug.Log("auto-store %v in the cache", h)
	err := b.cacheFile(ctx, h)
	if err == nil {
		return b.loadFromCacheOrDelegate(ctx, h, length, offset, consumer)
	}

	debug.Log("error caching %v: %v, falling back to backend", h, err)
	return b.CachedBackend.Load(ctx, h, length, offset, consumer)
}

// Stat tests whether the backend has a file. If it does not exist but still
// exists in the cache, it is removed from the cache.
func (b *Backend) Stat(ctx context.Context, h restic.Handle) (restic.FileInfo, error) {
	debug.Log("cache Stat(%v)", h)

	fi, err := b.CachedBackend.Stat(ctx, h)
	if err != nil {
		if b.CachedBackend.IsNotExist(err) {
			// try to remove from the cache, ignore errors
			_ = b.Cache.Remove(ctx, h)
		}

		return fi, err
	}

	return fi, err
}

// IsNotExist returns true if the error is caused by a non-existing file.
func (b *Backend) IsNotExist(err error) bool {
	return b.CachedBackend.IsNotExist(err)
}

// IsNotExist returns true if the error is caused by a non-existing file.
func (b *Backend) ClearCache(tpe restic.FileType, IDs restic.IDSet) error {
	// First clean all and then return errors if occured
	err1 := b.CachedBackend.ClearCache(tpe, IDs)
	err2 := b.Cache.Clear(tpe, IDs)
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func (b *Backend) IsCached(ctx context.Context, h restic.Handle) bool {
	return b.Cache.Has(ctx, h)
}
