package cache

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
)

type CacheLayout map[restic.FileType]struct {
	Path      string
	AutoCache bool
}

var LayoutStandard = CacheLayout{
	restic.PackFile:     {"data", false},
	restic.SnapshotFile: {"snapshots", true},
	restic.IndexFile:    {"index", true},
}

var LayoutAll = CacheLayout{
	restic.PackFile:     {"data", false},
	restic.SnapshotFile: {"snapshots", true},
	restic.IndexFile:    {"index", true},
	restic.KeyFile:      {"keys", true},
	restic.LockFile:     {"lock", true},
	restic.ConfigFile:   {"config", true},
}

// Cache manages a local cache.
type Cache struct {
	Path             string
	Base             string
	Created          bool
	PerformReadahead func(restic.Handle) bool
	Layout           CacheLayout
}

const dirMode = 0700
const fileMode = 0644

func readVersion(dir string) (v uint, err error) {
	buf, err := ioutil.ReadFile(filepath.Join(dir, "version"))
	if os.IsNotExist(err) {
		return 0, nil
	}

	if err != nil {
		return 0, errors.Wrap(err, "readVersion")
	}

	ver, err := strconv.ParseUint(string(buf), 10, 32)
	if err != nil {
		return 0, errors.Wrap(err, "readVersion")
	}

	return uint(ver), nil
}

const cacheVersion = 1
const cachedirTagSignature = "Signature: 8a477f597d28d172789f06886806bc55\n"

func writeCachedirTag(dir string) error {
	if err := fs.MkdirAll(dir, dirMode); err != nil {
		return errors.WithStack(err)
	}

	tagfile := filepath.Join(dir, "CACHEDIR.TAG")
	_, err := fs.Lstat(tagfile)
	if err != nil && !os.IsNotExist(err) {
		return errors.WithStack(err)
	}

	f, err := fs.OpenFile(tagfile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fileMode)
	if err != nil {
		if os.IsExist(errors.Cause(err)) {
			return nil
		}

		return errors.WithStack(err)
	}

	debug.Log("Create CACHEDIR.TAG at %v", dir)
	if _, err := f.Write([]byte(cachedirTagSignature)); err != nil {
		_ = f.Close()
		return errors.WithStack(err)
	}

	return errors.WithStack(f.Close())
}

// New returns a new cache for the repo ID at basedir. If basedir is the empty
// string, the default cache location (according to the XDG standard) is used.
//
// For partial files, the complete file is loaded and stored in the cache when
// performReadahead returns true.
func New(id string, basedir string, layout CacheLayout) (c *Cache, err error) {
	c = &Cache{
		PerformReadahead: func(restic.Handle) bool {
			// do not perform readahead by default
			return false
		},
		Layout: layout,
	}

	if basedir == "" {
		basedir, err = DefaultDir()
		if err != nil {
			return nil, err
		}
	}
	c.Base = basedir

	err = fs.MkdirAll(basedir, 0700)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create base dir and tag it as a cache directory
	if err = writeCachedirTag(basedir); err != nil {
		return nil, err
	}

	if !validCacheDirName(id) {
		return nil, errors.New("cache dir name is invalid")
	}

	cachedir := filepath.Join(basedir, id)
	c.Path = cachedir

	debug.Log("using cache dir %v", cachedir)

	v, err := readVersion(cachedir)
	if err != nil {
		return nil, err
	}

	if v > cacheVersion {
		return nil, errors.New("cache version is newer")
	}

	// create the repo cache dir if it does not exist yet
	var created bool
	_, err = fs.Lstat(cachedir)
	if os.IsNotExist(err) {
		err = fs.MkdirAll(cachedir, dirMode)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		created = true
	}
	c.Created = created

	// update the timestamp so that we can detect old cache dirs
	err = updateTimestamp(cachedir)
	if err != nil {
		return nil, err
	}

	if v < cacheVersion {
		err = ioutil.WriteFile(filepath.Join(cachedir, "version"), []byte(fmt.Sprintf("%d", cacheVersion)), fileMode)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	for _, p := range c.Layout {
		if err = fs.MkdirAll(filepath.Join(cachedir, p.Path), dirMode); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return c, nil
}

// updateTimestamp sets the modification timestamp (mtime and atime) for the
// directory d to the current time.
func updateTimestamp(d string) error {
	t := time.Now()
	return fs.Chtimes(d, t, t)
}

// MaxCacheAge is the default age (30 days) after which cache directories are considered old.
const MaxCacheAge = 30 * 24 * time.Hour

func validCacheDirName(s string) bool {
	r := regexp.MustCompile(`^(repo-)?[a-fA-F0-9]{64}$`)
	return r.MatchString(s)
}

// listCacheDirs returns the list of cache directories.
func listCacheDirs(basedir string) ([]os.FileInfo, error) {
	f, err := fs.Open(basedir)
	if err != nil && os.IsNotExist(errors.Cause(err)) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	entries, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}

	result := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if !validCacheDirName(entry.Name()) {
			continue
		}

		result = append(result, entry)
	}

	return result, nil
}

// All returns a list of cache directories.
func All(basedir string) (dirs []os.FileInfo, err error) {
	return listCacheDirs(basedir)
}

// OlderThan returns the list of cache directories older than max.
func OlderThan(basedir string, max time.Duration) ([]os.FileInfo, error) {
	entries, err := listCacheDirs(basedir)
	if err != nil {
		return nil, err
	}

	var oldCacheDirs []os.FileInfo
	for _, fi := range entries {
		if !IsOld(fi.ModTime(), max) {
			continue
		}

		oldCacheDirs = append(oldCacheDirs, fi)
	}

	debug.Log("%d old cache dirs found", len(oldCacheDirs))

	return oldCacheDirs, nil
}

// Old returns a list of cache directories with a modification time of more
// than 30 days ago.
func Old(basedir string) ([]os.FileInfo, error) {
	return OlderThan(basedir, MaxCacheAge)
}

// IsOld returns true if the timestamp is considered old.
func IsOld(t time.Time, maxAge time.Duration) bool {
	oldest := time.Now().Add(-maxAge)
	return t.Before(oldest)
}

// Wrap returns a backend with a cache.
func (c *Cache) Wrap(be restic.Backend) restic.Backend {
	return newBackend(be, c)
}

// BaseDir returns the base directory.
func (c *Cache) BaseDir() string {
	return c.Base
}
