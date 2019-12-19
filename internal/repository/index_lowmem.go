package repository

import (
	"context"
	"sync"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"

	"github.com/restic/restic/internal/debug"
)

// IndexLowMem holds a lookup table for id -> pack for tree blobs
// and a IDSet for data blobs
type IndexLowMem struct {
	m         sync.Mutex
	treePack  map[restic.BlobHandle]indexEntry
	dataBlobs restic.IDSet

	id         restic.ID // set to the ID of the index when it's finalized
	supersedes restic.IDs
	created    time.Time
	ctx        context.Context
	repo       restic.Repository
}

// NewIndexLowMem returns a new index.
func NewIndexLowMem() *IndexLowMem {
	return &IndexLowMem{
		treePack:  make(map[restic.BlobHandle]indexEntry),
		dataBlobs: restic.NewIDSet(),
		created:   time.Now(),
	}
}

func (idx *IndexLowMem) reloadIndex() (restic.FileIndex, error) {
	return LoadIndex(idx.ctx, idx.repo, idx.id)
}

func (idx *IndexLowMem) store(blob restic.PackedBlob) {
	switch blob.Type {
	case restic.DataBlob:
		idx.dataBlobs.Insert(blob.ID)
	case restic.TreeBlob:
		newEntry := indexEntry{
			packID: blob.PackID,
			offset: blob.Offset,
			length: blob.Length,
		}

		h := restic.BlobHandle{ID: blob.ID, Type: blob.Type}
		idx.treePack[h] = newEntry
	}

}

// Final returns true iff the index is already written to the repository, it is
// finalized.
func (idx *IndexLowMem) Final() bool {
	return true
}

// IsFull returns true iff the index is "full enough" to be saved as a preliminary index.
func (idx *IndexLowMem) IsFull() bool {
	return false
}

// Store remembers the id and pack in the index. An existing entry will be
// silently overwritten.
func (idx *IndexLowMem) Store(blob restic.PackedBlob) {
	panic("Store(..) cannot be called for Low-Mem-Index")
}

// Lookup queries the index for the blob ID and returns a restic.PackedBlob.
func (idx *IndexLowMem) Lookup(id restic.ID, tpe restic.BlobType) (blobs []restic.PackedBlob, found bool) {
	idx.m.Lock()
	defer idx.m.Unlock()

	switch tpe {
	case restic.DataBlob:
		// TODO error handling
		reloadIdx, _ := idx.reloadIndex()
		return reloadIdx.Lookup(id, tpe)
	case restic.TreeBlob:
		h := restic.BlobHandle{ID: id, Type: tpe}

		if p, ok := idx.treePack[h]; ok {
			blobs = make([]restic.PackedBlob, 0, 1)

			blobs = append(blobs, restic.PackedBlob{
				Blob: restic.Blob{
					Type:   tpe,
					Length: p.length,
					ID:     id,
					Offset: p.offset,
				},
				PackID: p.packID,
			})
			return blobs, true
		}
	}
	return nil, false
}

// ListPack returns a list of blobs contained in a pack.
func (idx *IndexLowMem) ListPack(id restic.ID) (list []restic.PackedBlob) {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.ListPack(id)
}

// Has returns true iff the id is listed in the index.
func (idx *IndexLowMem) Has(id restic.ID, tpe restic.BlobType) bool {
	idx.m.Lock()
	defer idx.m.Unlock()

	switch tpe {
	case restic.DataBlob:
		return idx.dataBlobs.Has(id)
	case restic.TreeBlob:
		h := restic.BlobHandle{ID: id, Type: tpe}
		_, ok := idx.treePack[h]
		return ok
	}
	return false
}

// LookupSize returns the length of the plaintext content of the blob with the
// given id.
func (idx *IndexLowMem) LookupSize(id restic.ID, tpe restic.BlobType) (plaintextLength uint, found bool) {
	blobs, found := idx.Lookup(id, tpe)
	if !found {
		return 0, found
	}

	return uint(restic.PlaintextLength(int(blobs[0].Length))), true
}

// Supersedes returns the list of indexes this index supersedes, if any.
func (idx *IndexLowMem) Supersedes() restic.IDs {
	return idx.supersedes
}

// AddToSupersedes adds the ids to the list of indexes superseded by this
// index. If the index has already been finalized, an error is returned.
func (idx *IndexLowMem) AddToSupersedes(ids ...restic.ID) error {
	return errors.New("index already finalized")

}

// Each returns a channel that yields all blobs known to the index. When the
// context is cancelled, the background goroutine terminates. This blocks any
// modification of the index.
func (idx *IndexLowMem) Each(ctx context.Context) <-chan restic.PackedBlob {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.Each(ctx)
}

// Each returns a channel that yields all blobs known to the index. When the
// context is cancelled, the background goroutine terminates. This blocks any
// modification of the index.
func (idx *IndexLowMem) EachBlobHandle(ctx context.Context) <-chan restic.BlobHandle {
	idx.m.Lock()
	ch := make(chan restic.BlobHandle)

	go func() {
		defer idx.m.Unlock()
		defer func() {
			close(ch)
		}()

		for h := range idx.treePack {
			select {
			case <-ctx.Done():
				return
			case ch <- h:
			}
		}

		for id := range idx.dataBlobs {
			select {
			case <-ctx.Done():
				return
			case ch <- restic.BlobHandle{ID: id, Type: restic.DataBlob}:
			}

		}
	}()

	return ch
}

// Packs returns all packs in this index
func (idx *IndexLowMem) Packs() restic.IDSet {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO error handling
	reloadIdx, _ := idx.reloadIndex()
	return reloadIdx.Packs()
}

// Count returns the number of blobs of type t in the index.
func (idx *IndexLowMem) Count(t restic.BlobType) (n uint) {

	debug.Log("counting blobs of type %v", t)
	idx.m.Lock()
	defer idx.m.Unlock()

	switch t {
	case restic.DataBlob:
		return uint(len(idx.dataBlobs))
	case restic.TreeBlob:
		return uint(len(idx.treePack))
	}

	return
}

// ID returns the ID of the index, if available. If the index is not yet
// finalized, an error is returned.
func (idx *IndexLowMem) ID() (restic.ID, error) {
	idx.m.Lock()
	defer idx.m.Unlock()

	return idx.id, nil
}

// SetID sets the ID the index has been written to. This requires that
// Finalize() has been called before, otherwise an error is returned.
func (idx *IndexLowMem) SetID(id restic.ID) error {
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
func (idx *IndexLowMem) TreePacks() restic.IDs {
	treePacks := make(restic.IDs, 0, len(idx.treePack))
	for bh := range idx.treePack {
		treePacks = append(treePacks, bh.ID)
	}
	return treePacks
}

// LoadIndexLowMemFromIndex loads the index from an standard index.
func MakeIndexLowMemFromIndex(ctx context.Context, repo restic.Repository, idx restic.FileIndex) *IndexLowMem {

	indexLM := NewIndexLowMem()

	indexLM.ctx = ctx
	indexLM.repo = repo
	// TODO: error handling
	indexLM.id, _ = idx.ID()
	for pb := range idx.Each(ctx) {
		indexLM.store(pb)
	}

	return indexLM
}
