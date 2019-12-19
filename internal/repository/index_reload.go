package repository

import (
	"context"
	"sync"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"

	"github.com/restic/restic/internal/debug"
)

// IndexReload reloads the index for every operation

type IndexReload struct {
	m sync.Mutex

	id      restic.ID // set to the ID of the index when it's finalized
	created time.Time
	ctx     context.Context
	repo    restic.Repository
}

// NewIndexReload returns a new index.
func NewIndexReload() *IndexReload {
	return &IndexReload{
		created: time.Now(),
	}
}

func (idx *IndexReload) reloadIndex() (restic.FileIndex, error) {
	return LoadIndex(idx.ctx, idx.repo, idx.id)
}

func (idx *IndexReload) store(blob restic.PackedBlob) {
}

// Final returns true iff the index is already written to the repository, it is
// finalized.
func (idx *IndexReload) Final() bool {
	return true
}

// IsFull returns true iff the index is "full enough" to be saved as a preliminary index.
func (idx *IndexReload) IsFull() bool {
	return false
}

// Store remembers the id and pack in the index. An existing entry will be
// silently overwritten.
func (idx *IndexReload) Store(blob restic.PackedBlob) {
	panic("Store(..) cannot be called for Reload-Index")
}

// Lookup queries the index for the blob ID and returns a restic.PackedBlob.
func (idx *IndexReload) Lookup(id restic.ID, tpe restic.BlobType) (blobs []restic.PackedBlob, found bool) {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.Lookup(id, tpe)
}

// ListPack returns a list of blobs contained in a pack.
func (idx *IndexReload) ListPack(id restic.ID) (list []restic.PackedBlob) {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.ListPack(id)
}

// Has returns true iff the id is listed in the index.
func (idx *IndexReload) Has(id restic.ID, tpe restic.BlobType) bool {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.Has(id, tpe)
}

// LookupSize returns the length of the plaintext content of the blob with the
// given id.
func (idx *IndexReload) LookupSize(id restic.ID, tpe restic.BlobType) (plaintextLength uint, found bool) {
	blobs, found := idx.Lookup(id, tpe)
	if !found {
		return 0, found
	}

	return uint(restic.PlaintextLength(int(blobs[0].Length))), true
}

// Supersedes returns the list of indexes this index supersedes, if any.
func (idx *IndexReload) Supersedes() restic.IDs {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.Supersedes()
}

// AddToSupersedes adds the ids to the list of indexes superseded by this
// index. If the index has already been finalized, an error is returned.
func (idx *IndexReload) AddToSupersedes(ids ...restic.ID) error {
	return errors.New("index already finalized")

}

// Each returns a channel that yields all blobs known to the index. When the
// context is cancelled, the background goroutine terminates. This blocks any
// modification of the index.
func (idx *IndexReload) Each(ctx context.Context) <-chan restic.PackedBlob {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.Each(ctx)
}

// Each returns a channel that yields all blobs known to the index. When the
// context is cancelled, the background goroutine terminates. This blocks any
// modification of the index.
func (idx *IndexReload) EachBlobHandle(ctx context.Context) <-chan restic.BlobHandle {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.EachBlobHandle(ctx)
}

// Packs returns all packs in this index
func (idx *IndexReload) Packs() restic.IDSet {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.Packs()
}

// Count returns the number of blobs of type t in the index.
func (idx *IndexReload) Count(t restic.BlobType) (n uint) {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.Count(t)
}

// ID returns the ID of the index, if available. If the index is not yet
// finalized, an error is returned.
func (idx *IndexReload) ID() (restic.ID, error) {
	idx.m.Lock()
	defer idx.m.Unlock()

	return idx.id, nil
}

// SetID sets the ID the index has been written to. This requires that
// Finalize() has been called before, otherwise an error is returned.
func (idx *IndexReload) SetID(id restic.ID) error {
	idx.m.Lock()
	defer idx.m.Unlock()

	if !idx.id.IsNull() {
		return errors.New("ID already set")
	}

	debug.Log("ID set to %v", id)
	idx.id = id

	return nil
}

// TreePacks returns a list of packs that contain only tree blobs.
func (idx *IndexReload) TreePacks() restic.IDs {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.TreePacks()
}

// LoadIndexReloadFromIndex loads the index from an standard index.
func MakeIndexReloadFromIndex(ctx context.Context, repo restic.Repository, idx restic.FileIndex) *IndexReload {

	indexLM := NewIndexReload()

	indexLM.ctx = ctx
	indexLM.repo = repo
	// TODO: error handling
	indexLM.id, _ = idx.ID()

	return indexLM
}
