package restic

import (
	"context"
	"fmt"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/restic/restic/internal/debug"
)

// Snapshot is the state of a resource at one point in time.
type Snapshot struct {
	Time     time.Time `json:"time"`
	Parent   *ID       `json:"parent,omitempty"`
	Parents  IDs       `json:"parents,omitempty"`
	Tree     *ID       `json:"tree"`
	Paths    []string  `json:"paths"`
	Hostname string    `json:"hostname,omitempty"`
	Username string    `json:"username,omitempty"`
	UID      uint32    `json:"uid,omitempty"`
	GID      uint32    `json:"gid,omitempty"`
	Excludes []string  `json:"excludes,omitempty"`
	Tags     []string  `json:"tags,omitempty"`
	Original *ID       `json:"original,omitempty"`

	id *ID // plaintext ID, used during restore
}

// NewSnapshot returns an initialized snapshot struct for the current user and
// time.
func NewSnapshot(paths []string, tags []string, hostname string, time time.Time) (*Snapshot, error) {
	absPaths := make([]string, 0, len(paths))
	for _, path := range paths {
		p, err := filepath.Abs(path)
		if err == nil {
			absPaths = append(absPaths, p)
		} else {
			absPaths = append(absPaths, path)
		}
	}

	sn := &Snapshot{
		Paths:    absPaths,
		Time:     time,
		Tags:     tags,
		Hostname: hostname,
	}

	err := sn.fillUserInfo()
	if err != nil {
		return nil, err
	}

	return sn, nil
}

// LoadSnapshot loads the snapshot with the id and returns it.
func LoadSnapshot(ctx context.Context, repo Repository, id ID) (*Snapshot, error) {
	sn := &Snapshot{id: &id}
	err := repo.LoadJSONUnpacked(ctx, SnapshotFile, id, sn)
	if err != nil {
		return nil, err
	}

	return sn, nil
}

const loadSnapshotParallelism = 5

// ForAllSnapshots reads all snapshots in parallel and calls the
// given function. It is guaranteed that the function is not run concurrently.
// If the called function returns an error, this function is cancelled and
// also returns this error.
// If a snapshot ID is in excludeIDs, it will be ignored.
func ForAllSnapshots(ctx context.Context, repo Repository, excludeIDs IDSet, fn func(ID, *Snapshot, error) error) error {
	var m sync.Mutex

	// track spawned goroutines using wg, create a new context which is
	// cancelled as soon as an error occurs.
	wg, ctx := errgroup.WithContext(ctx)

	ch := make(chan ID)

	// send list of snapshot files through ch, which is closed afterwards
	wg.Go(func() error {
		defer close(ch)
		return repo.List(ctx, SnapshotFile, func(id ID, size int64) error {
			if excludeIDs.Has(id) {
				return nil
			}

			select {
			case <-ctx.Done():
				return nil
			case ch <- id:
			}
			return nil
		})
	})

	// a worker receives an snapshot ID from ch, loads the snapshot
	// and runs fn with id, the snapshot and the error
	worker := func() error {
		for id := range ch {
			debug.Log("load snapshot %v", id)
			sn, err := LoadSnapshot(ctx, repo, id)

			m.Lock()
			err = fn(id, sn, err)
			m.Unlock()
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := 0; i < loadSnapshotParallelism; i++ {
		wg.Go(worker)
	}

	return wg.Wait()
}

func (sn Snapshot) String() string {
	return fmt.Sprintf("<Snapshot %s of %v at %s by %s@%s>",
		sn.id.Str(), sn.Paths, sn.Time, sn.Username, sn.Hostname)
}

// ID returns the snapshot's ID.
func (sn Snapshot) ID() *ID {
	return sn.id
}

func (sn *Snapshot) fillUserInfo() error {
	usr, err := user.Current()
	if err != nil {
		return nil
	}
	sn.Username = usr.Username

	// set userid and groupid
	sn.UID, sn.GID, err = uidGidInt(*usr)
	return err
}

// AddTags adds the given tags to the snapshots tags, preventing duplicates.
// It returns true if any changes were made.
func (sn *Snapshot) AddTags(addTags []string) (changed bool) {
nextTag:
	for _, add := range addTags {
		for _, tag := range sn.Tags {
			if tag == add {
				continue nextTag
			}
		}
		sn.Tags = append(sn.Tags, add)
		changed = true
	}
	return
}

// RemoveTags removes the given tags from the snapshots tags and
// returns true if any changes were made.
func (sn *Snapshot) RemoveTags(removeTags []string) (changed bool) {
	for _, remove := range removeTags {
		for i, tag := range sn.Tags {
			if tag == remove {
				// https://github.com/golang/go/wiki/SliceTricks
				sn.Tags[i] = sn.Tags[len(sn.Tags)-1]
				sn.Tags[len(sn.Tags)-1] = ""
				sn.Tags = sn.Tags[:len(sn.Tags)-1]

				changed = true
				break
			}
		}
	}
	return
}

func (sn *Snapshot) hasTag(tag string) bool {
	for _, snTag := range sn.Tags {
		if tag == snTag {
			return true
		}
	}
	return false
}

// HasTags returns true if the snapshot has all the tags in l.
func (sn *Snapshot) HasTags(l []string) bool {
	for _, tag := range l {
		if tag == "" && len(sn.Tags) == 0 {
			return true
		}
		if !sn.hasTag(tag) {
			return false
		}
	}

	return true
}

// HasTagList returns true if either
// - the snapshot satisfies at least one TagList, so there is a TagList in l
//   for which all tags are included in sn, or
// - l is empty
func (sn *Snapshot) HasTagList(l []TagList) bool {
	debug.Log("testing snapshot with tags %v against list: %v", sn.Tags, l)

	if len(l) == 0 {
		return true
	}

	for _, tags := range l {
		if sn.HasTags(tags) {
			debug.Log("  snapshot satisfies %v %v", tags, l)
			return true
		}
	}

	return false
}

func (sn *Snapshot) hasPath(path string) bool {
	for _, snPath := range sn.Paths {
		if path == snPath {
			return true
		}
	}
	return false
}

// HasPaths returns true if the snapshot has all of the paths.
func (sn *Snapshot) HasPaths(paths []string) bool {
	for _, path := range paths {
		if !sn.hasPath(path) {
			return false
		}
	}

	return true
}

// isSubPath returns whether path is a subpath of givenPath
func isSubPath(path, givenPath string) bool {
	pattern := givenPath + string(filepath.Separator) + "*"
	matched, _ := filepath.Match(pattern, path)
	return matched
}

// matchPath returns true iff both paths are identical
// or one is a subpath of the other
func matchPath(path, givenPath string) bool {
	if path == givenPath || isSubPath(givenPath, path) || isSubPath(path, givenPath) {
		return true
	}
	return false
}

func supersedes(path1, path2, givenPath string) bool {
	if path1 == givenPath || isSubPath(givenPath, path1) ||
		(isSubPath(path1, givenPath) && isSubPath(path2, path1)) {
		return true
	}
	return false
}

// MatchPath returns true iff at least one path of the snapshot matches
// the given path.
func (sn *Snapshot) MatchPath(path string) bool {
	for _, snPath := range sn.Paths {
		if matchPath(path, snPath) {
			return true
		}
	}
	return false
}

// MatchPaths returns true iff the snapshot matches at least one path
// of the given paths.
func (sn *Snapshot) MatchPaths(paths []string) bool {
	for _, path := range paths {
		if sn.MatchPath(path) {
			return true
		}
	}
	return false
}

// Supersedes returns whether sn supersedes sn2 w.r.t. of the given paths:
// This is the case iff:
// - the snapshot is not older
// - for all given paths, either
//   * sn2 does not match this path or
//   * sn has either an identical path or a superpath
//     (this implies that sn1 also matches that path)
// Note that Supersedes() with fixed given paths defines a partial order on
// a set of snapshots.
func (sn *Snapshot) Supersedes(sn2 *Snapshot, paths []string) bool {
	if sn.Time.Before(sn2.Time) {
		return false
	}
	for _, path := range paths {
		for _, sn2Path := range sn2.Paths {
			// ignore paths which sn2 does not match
			if !matchPath(sn2Path, path) {
				continue
			}
			foundPath := false
			for _, sn1Path := range sn.Paths {
				// sn1 supersedes
				if supersedes(sn1Path, sn2Path, path) {
					foundPath = true
					break
				}
			}
			if !foundPath {
				return false
			}
		}
	}
	return true
}

// HasHostname returns true if either
// - the snapshot hostname is in the list of the given hostnames, or
// - the list of given hostnames is empty
func (sn *Snapshot) HasHostname(hostnames []string) bool {
	if len(hostnames) == 0 {
		return true
	}

	for _, hostname := range hostnames {
		if sn.Hostname == hostname {
			return true
		}
	}

	return false
}

// Snapshots is a list of snapshots.
type Snapshots []*Snapshot

// Len returns the number of snapshots in sn.
func (sn Snapshots) Len() int {
	return len(sn)
}

// Less returns true iff the ith snapshot has been made after the jth.
func (sn Snapshots) Less(i, j int) bool {
	return sn[i].Time.After(sn[j].Time)
}

// Swap exchanges the two snapshots.
func (sn Snapshots) Swap(i, j int) {
	sn[i], sn[j] = sn[j], sn[i]
}
