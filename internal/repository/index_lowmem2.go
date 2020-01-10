package repository

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"

	"github.com/restic/restic/internal/debug"
)

type blobWithoutType struct {
	blobID    restic.ID
	packIndex int
	offset    uint
	length    uint
}

type indexTable struct {
	packTable    restic.IDs
	blobTable    []blobWithoutType
	sortedByBlob bool
	sortedByPack bool
}

func (bt *indexTable) FindPack(id restic.ID) (int, bool) {
	i := sort.Search(len(bt.packTable), func(i int) bool {
		return id.LessEqual(bt.packTable[i])
	})
	if i < len(bt.packTable) && bt.packTable[i] == id {
		return i, true
	}
	return i, false
}

func (bt *indexTable) SortByBlob() {
	sort.Slice(bt.blobTable, func(i, j int) bool {
		return bt.blobTable[i].blobID.Less(bt.blobTable[j].blobID)
	})
	bt.sortedByBlob = true
	bt.sortedByPack = false
}

func (bt *indexTable) SortByPack() {
	// Sort using stable sorting algorithm, so by Blob sorted Table is still
	// sorted by Blob within pack!
	sort.SliceStable(bt.blobTable, func(i, j int) bool {
		return bt.blobTable[i].packIndex < bt.blobTable[j].packIndex
	})
	bt.sortedByPack = true
	bt.sortedByBlob = false
}

func (bt *indexTable) FindBlob(id restic.ID) (int, bool) {
	if !bt.sortedByBlob {
		bt.SortByBlob()
	}
	i := sort.Search(len(bt.blobTable), func(i int) bool {
		return id.LessEqual(bt.blobTable[i].blobID)
	})
	if i < len(bt.blobTable) && bt.blobTable[i].blobID == id {
		return i, true
	}
	return i, false
}

func (bt *indexTable) FindBlobByPack(id restic.ID) (int, int, bool) {
	packindex, ok := bt.FindPack(id)
	if !ok {
		return len(bt.blobTable), -1, false
	}
	if !bt.sortedByPack {
		bt.SortByPack()
	}
	i := sort.Search(len(bt.blobTable), func(i int) bool {
		return bt.blobTable[i].packIndex >= packindex
	})
	if i < len(bt.blobTable) && bt.blobTable[i].packIndex == packindex {
		return i, packindex, true
	}
	return i, packindex, false
}

// IndexLowMem2 holds a lookup table for id -> pack for tree blobs
// and a IDSet for data blobs
type IndexLowMem2 struct {
	m      sync.Mutex
	byType map[restic.BlobType]*indexTable

	id         restic.ID // set to the ID of the index when it's finalized
	supersedes restic.IDs
	created    time.Time
}

// NewIndexLowMem2 returns a new index.
func NewIndexLowMem2() *IndexLowMem2 {
	return &IndexLowMem2{
		created: time.Now(),
		byType:  make(map[restic.BlobType]*indexTable),
	}
}

// Final returns true iff the index is already written to the repository, it is
// finalized.
func (idx *IndexLowMem2) Final() bool {
	return true
}

// IsFull returns true iff the index is "full enough" to be saved as a preliminary index.
func (idx *IndexLowMem2) IsFull() bool {
	return false
}

// Store remembers the id and pack in the index. An existing entry will be
// silently overwritten.
func (idx *IndexLowMem2) Store(blob restic.PackedBlob) {
	panic("Store(..) cannot be called for Low-Mem-Index")
}

// Lookup queries the index for the blob ID and returns a restic.PackedBlob.
func (idx *IndexLowMem2) Lookup(id restic.ID, tpe restic.BlobType) (blobs []restic.PackedBlob, found bool) {
	idx.m.Lock()
	defer idx.m.Unlock()

	// TODO: return all instead of one pack => is this needed?
	it, _ := idx.byType[tpe]
	i, ok := it.FindBlob(id)
	if ok {
		blob := it.blobTable[i]
		pb := restic.PackedBlob{
			Blob: restic.Blob{
				Type:   tpe,
				Length: blob.length,
				ID:     id,
				Offset: blob.offset,
			},
			PackID: it.packTable[blob.packIndex],
		}
		packedBlobs := make([]restic.PackedBlob, 0, 1)
		packedBlobs = append(packedBlobs, pb)
		return packedBlobs, true
	}
	return nil, false
}

// ListPack returns a list of blobs contained in a pack.
func (idx *IndexLowMem2) ListPack(id restic.ID) (list []restic.PackedBlob) {
	idx.m.Lock()
	defer idx.m.Unlock()

	for tpe, it := range idx.byType {
		i, packindex, ok := it.FindBlobByPack(id)
		if !ok {
			continue
		}
		for ; it.blobTable[i].packIndex == packindex; i++ {
			blob := it.blobTable[i]
			pb := restic.PackedBlob{
				Blob: restic.Blob{
					Type:   tpe,
					Length: blob.length,
					ID:     blob.blobID,
					Offset: blob.offset,
				},
				PackID: id,
			}
			list = append(list, pb)
		}
	}

	return list
}

// Has returns true iff the id is listed in the index.
func (idx *IndexLowMem2) Has(id restic.ID, tpe restic.BlobType) bool {
	idx.m.Lock()
	defer idx.m.Unlock()

	bt, ok := idx.byType[tpe]
	if !ok {
		return false
	}

	if _, ok := bt.FindBlob(id); ok {
		return true
	}
	return false
}

// LookupSize returns the length of the plaintext content of the blob with the
// given id.
func (idx *IndexLowMem2) LookupSize(id restic.ID, tpe restic.BlobType) (plaintextLength uint, found bool) {
	blobs, found := idx.Lookup(id, tpe)
	if !found {
		return 0, false
	}
	return uint(restic.PlaintextLength(int(blobs[0].Length))), true
}

// Supersedes returns the list of indexes this index supersedes, if any.
func (idx *IndexLowMem2) Supersedes() restic.IDs {
	return idx.supersedes
}

// AddToSupersedes adds the ids to the list of indexes superseded by this
// index. If the index has already been finalized, an error is returned.
func (idx *IndexLowMem2) AddToSupersedes(ids ...restic.ID) error {
	return errors.New("index already finalized")
}

// Each returns a channel that yields all blobs known to the index. When the
// context is cancelled, the background goroutine terminates. This blocks any
// modification of the index.
func (idx *IndexLowMem2) Each(ctx context.Context) <-chan restic.PackedBlob {
	idx.m.Lock()
	defer idx.m.Unlock()
	ch := make(chan restic.PackedBlob)

	go func() {
		defer idx.m.Unlock()
		defer func() {
			close(ch)
		}()

		for tpe, it := range idx.byType {
			for _, blob := range it.blobTable {
				pb := restic.PackedBlob{
					restic.Blob{
						ID:     blob.blobID,
						Type:   tpe,
						Length: blob.length,
						Offset: blob.offset,
					},
					it.packTable[blob.packIndex],
				}
				select {
				case <-ctx.Done():
					return
				case ch <- pb:
				}
			}
		}
	}()

	return ch
}

// Each returns a channel that yields all blobs known to the index. When the
// context is cancelled, the background goroutine terminates. This blocks any
// modification of the index.
func (idx *IndexLowMem2) EachBlobHandle(ctx context.Context) <-chan restic.BlobHandle {
	idx.m.Lock()
	ch := make(chan restic.BlobHandle)

	go func() {
		defer idx.m.Unlock()
		defer func() {
			close(ch)
		}()

		for tpe := range idx.byType {
			for _, blob := range idx.byType[tpe].blobTable {
				bh := restic.BlobHandle{ID: blob.blobID, Type: tpe}
				select {
				case <-ctx.Done():
					return
				case ch <- bh:
				}
			}
		}
	}()

	return ch
}

// Packs returns all packs in this index
func (idx *IndexLowMem2) Packs() restic.IDSet {
	idx.m.Lock()
	defer idx.m.Unlock()

	packs := restic.NewIDSet()
	for _, it := range idx.byType {
		for _, id := range it.packTable {
			packs.Insert(id)
		}
	}
	return packs
}

// Count returns the number of blobs of type t in the index.
func (idx *IndexLowMem2) Count(t restic.BlobType) (n uint) {

	debug.Log("counting blobs of type %v", t)
	idx.m.Lock()
	defer idx.m.Unlock()

	return uint(len(idx.byType[t].blobTable))
}

// ID returns the ID of the index, if available. If the index is not yet
// finalized, an error is returned.
func (idx *IndexLowMem2) ID() (restic.ID, error) {
	idx.m.Lock()
	defer idx.m.Unlock()

	return idx.id, nil
}

// SetID sets the ID the index has been written to. This requires that
// Finalize() has been called before, otherwise an error is returned.
func (idx *IndexLowMem2) SetID(id restic.ID) error {
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
func (idx *IndexLowMem2) TreePacks() restic.IDs {
	return idx.byType[restic.TreeBlob].packTable
}

// LoadIndexLowMem2FromIndex loads the index from an standard index.
func MakeIndexLowMem2FromIndex(ctx context.Context, repo restic.Repository, idx restic.FileIndex) *IndexLowMem2 {

	indexLM := NewIndexLowMem2()

	// TODO: error handling
	indexLM.id, _ = idx.ID()

	// Auxiliary structures to get all packs, used types
	// and number of blobs of each type
	packs := make(map[restic.BlobType]restic.IDSet)
	sizes := make(map[restic.BlobType]int)

	// First scan of index: Get packs and count
	for pb := range idx.Each(ctx) {
		if _, ok := packs[pb.Type]; !ok {
			packs[pb.Type] = restic.NewIDSet()
		}
		packs[pb.Type].Insert(pb.PackID)
		sizes[pb.Type]++
	}

	for tpe, size := range sizes {
		// fill packTable
		packTable := make(restic.IDs, 0, len(packs[tpe]))
		for id, _ := range packs[tpe] {
			packTable = append(packTable, id)
		}
		// and sort it
		sort.Sort(packTable)

		// allocate blobTable
		it := indexTable{
			packTable: packTable,
			blobTable: make([]blobWithoutType, 0, size),
		}
		indexLM.byType[tpe] = &it

		// Blobtable is not sorted by default
		indexLM.byType[tpe].sortedByBlob = false
		indexLM.byType[tpe].sortedByPack = false
	}

	// second scan: fill BlobTable
	for pb := range idx.Each(ctx) {
		packindex, ok := indexLM.byType[pb.Type].FindPack(pb.PackID)
		if !ok {
			// should not happen; pack ID should be in PackTable
			panic("packindex not found!")
		}
		b := blobWithoutType{
			blobID:    pb.ID,
			packIndex: packindex,
			offset:    pb.Offset,
			length:    pb.Length,
		}
		indexLM.byType[pb.Type].blobTable = append(indexLM.byType[pb.Type].blobTable, b)
	}

	return indexLM
}
