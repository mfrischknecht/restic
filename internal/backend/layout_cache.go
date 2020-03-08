package backend

import (
	"encoding/hex"

	"github.com/restic/restic/internal/restic"
)

// CacheLayout implements the layout used for the cache
// All directories have one level of subdirs, two characters each
// (taken from the first two characters of the file name).
type CacheLayout struct {
	Path string
	Join func(...string) string
}

var cacheLayoutPaths = map[restic.FileType]string{
	restic.DataFile:     "data",
	restic.SnapshotFile: "snapshots",
	restic.IndexFile:    "index",
}

func (l *CacheLayout) String() string {
	return "<CacheLayout>"
}

// Name returns the name for this layout.
func (l *CacheLayout) Name() string {
	return "default"
}

// Dirname returns the directory path for a given file type and name.
func (l *CacheLayout) Dirname(h restic.Handle) string {
	p := cacheLayoutPaths[h.Type]

	if len(h.Name) > 2 {
		p = l.Join(p, h.Name[:2]) + "/"
	}

	return l.Join(l.Path, p) + "/"
}

// Filename returns a path to a file, including its name.
func (l *CacheLayout) Filename(h restic.Handle) string {
	return l.Join(l.Dirname(h), h.Name)
}

// Paths returns all directory names needed for a repo.
func (l *CacheLayout) Paths() (dirs []string) {
	for _, p := range cacheLayoutPaths {
		path := l.Join(l.Path, p)
		dirs = append(dirs, path)
		// also add subdirs
		for i := 0; i < 256; i++ {
			subdir := hex.EncodeToString([]byte{byte(i)})
			dirs = append(dirs, l.Join(path, subdir))
		}
	}

	return dirs
}

// Basedir returns the base dir name for type t.
func (l *CacheLayout) Basedir(t restic.FileType) (dirname string, subdirs bool) {
	subdirs = true
	dirname = l.Join(l.Path, cacheLayoutPaths[t])
	return
}
