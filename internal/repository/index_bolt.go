package repository

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"

	"github.com/restic/restic/internal/debug"

	bolt "github.com/etcd-io/bbolt"
)

var (
	db *bolt.DB
)

func init() {
	var err error
	db, err = bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
}

// IndexBolt holds a lookup table for id -> pack for tree blobs
// and a IDSet for data blobs
type IndexBolt struct {
	id  restic.ID // set to the ID of the index when it's finalized
	ctx context.Context

	supersedes restic.IDs
	created    time.Time
}

type BoltindexEntry struct {
	PackID restic.ID
	Offset uint
	Length uint
}

// NewIndexBolt returns a new index.
func NewIndexBolt() *IndexBolt {
	return &IndexBolt{
		created: time.Now(),
	}
}

func handleToBytes(blob restic.BlobHandle) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	// TODO: Error handling
	enc.Encode(blob)
	return buf.Bytes()
}

func bytesToIndexEntry(data []byte) BoltindexEntry {
	var entry BoltindexEntry
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	// TODO: Error handling
	dec.Decode(&entry)
	return entry
}

func bytesToHandle(data []byte) restic.BlobHandle {
	var handle restic.BlobHandle
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	// TODO: Error handling
	dec.Decode(&handle)
	return handle
}

func indexEntryToBytes(ie BoltindexEntry) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	// TODO: Error handling
	enc.Encode(ie)
	return buf.Bytes()
}

func (idx *IndexBolt) IdToBytes() []byte {
	return []byte(idx.id.String())
}

func (idx *IndexBolt) getEntry(blob restic.BlobHandle) (BoltindexEntry, bool) {
	var entry BoltindexEntry
	var found bool
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(idx.IdToBytes())
		result := b.Get(handleToBytes(blob))
		if result != nil {
			found = true
			entry = bytesToIndexEntry(result)
		} else {
			found = false
		}
		return nil
	})
	return entry, found
}

func (idx *IndexBolt) store(blob restic.PackedBlob) {
	newEntry := BoltindexEntry{
		PackID: blob.PackID,
		Offset: blob.Offset,
		Length: blob.Length,
	}
	h := restic.BlobHandle{ID: blob.ID, Type: blob.Type}
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(idx.IdToBytes())
		err := b.Put(handleToBytes(h), indexEntryToBytes(newEntry))
		return err
	})
}

// Final returns true iff the index is already written to the repository, it is
// finalized.
func (idx *IndexBolt) Final() bool {
	return true
}

// IsFull returns true iff the index is "full enough" to be saved as a preliminary index.
func (idx *IndexBolt) IsFull() bool {
	return false
}

// Store remembers the id and pack in the index. An existing entry will be
// silently overwritten.
func (idx *IndexBolt) Store(blob restic.PackedBlob) {
	panic("Store(..) cannot be called for Bolt-Index")
}

// Lookup queries the index for the blob ID and returns a restic.PackedBlob.
func (idx *IndexBolt) Lookup(id restic.ID, tpe restic.BlobType) (blobs []restic.PackedBlob, found bool) {
	h := restic.BlobHandle{ID: id, Type: tpe}
	if p, ok := idx.getEntry(h); ok {
		blobs = make([]restic.PackedBlob, 0, 1)

		blobs = append(blobs, restic.PackedBlob{
			Blob: restic.Blob{
				Type:   tpe,
				Length: p.Length,
				ID:     id,
				Offset: p.Offset,
			},
			PackID: p.PackID,
		})
		return blobs, true
	}
	return nil, false
}

// ListPack returns a list of blobs contained in a pack.
func (idx *IndexBolt) ListPack(id restic.ID) (list []restic.PackedBlob) {
	for pb := range idx.Each(idx.ctx) {
		if pb.PackID == id {
			list = append(list, pb)
		}
	}
	return list
}

// Has returns true iff the id is listed in the index.
func (idx *IndexBolt) Has(id restic.ID, tpe restic.BlobType) bool {
	h := restic.BlobHandle{ID: id, Type: tpe}
	if _, ok := idx.getEntry(h); ok {
		return true
	}
	return false
}

// LookupSize returns the length of the plaintext content of the blob with the
// given id.
func (idx *IndexBolt) LookupSize(id restic.ID, tpe restic.BlobType) (plaintextLength uint, found bool) {
	blobs, found := idx.Lookup(id, tpe)
	if !found {
		return 0, found
	}

	return uint(restic.PlaintextLength(int(blobs[0].Length))), true
}

// Supersedes returns the list of indexes this index supersedes, if any.
func (idx *IndexBolt) Supersedes() restic.IDs {
	return idx.supersedes
}

// AddToSupersedes adds the ids to the list of indexes superseded by this
// index. If the index has already been finalized, an error is returned.
func (idx *IndexBolt) AddToSupersedes(ids ...restic.ID) error {
	return errors.New("index already finalized")
}

// Each returns a channel that yields all blobs known to the index. When the
// context is cancelled, the background goroutine terminates. This blocks any
// modification of the index.
func (idx *IndexBolt) Each(ctx context.Context) <-chan restic.PackedBlob {
	ch := make(chan restic.PackedBlob)

	go func() {
		defer func() {
			close(ch)
		}()

		db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket(idx.IdToBytes())

			c := b.Cursor()

			for k, v := c.First(); k != nil; k, v = c.Next() {
				h := bytesToHandle(k)
				ie := bytesToIndexEntry(v)
				select {
				case <-ctx.Done():
					return nil
				case ch <- restic.PackedBlob{
					Blob: restic.Blob{
						ID:     h.ID,
						Type:   h.Type,
						Offset: ie.Offset,
						Length: ie.Length,
					},
					PackID: ie.PackID,
				}:
				}
			}
			return nil
		})
	}()

	return ch
}

// Each returns a channel that yields all blobs known to the index. When the
// context is cancelled, the background goroutine terminates. This blocks any
// modification of the index.
func (idx *IndexBolt) EachBlobHandle(ctx context.Context) <-chan restic.BlobHandle {
	ch := make(chan restic.BlobHandle)

	go func() {
		defer func() {
			close(ch)
		}()

		db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket(idx.IdToBytes())

			c := b.Cursor()

			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				h := bytesToHandle(k)
				select {
				case <-ctx.Done():
					return nil
				case ch <- restic.BlobHandle{
					ID:   h.ID,
					Type: h.Type,
				}:
				}
			}
			return nil
		})
	}()
	return ch
}

// Packs returns all packs in this index
func (idx *IndexBolt) Packs() restic.IDSet {
	packs := restic.NewIDSet()

	for pb := range idx.Each(idx.ctx) {
		packs.Insert(pb.PackID)
	}

	return packs
}

// Count returns the number of blobs of type t in the index.
func (idx *IndexBolt) Count(t restic.BlobType) (n uint) {
	for pb := range idx.Each(idx.ctx) {
		if pb.Type == t {
			n++
		}
	}
	return n

}

// ID returns the ID of the index, if available. If the index is not yet
// finalized, an error is returned.
func (idx *IndexBolt) ID() (restic.ID, error) {
	return idx.id, nil
}

// SetID sets the ID the index has been written to. This requires that
// Finalize() has been called before, otherwise an error is returned.
func (idx *IndexBolt) SetID(id restic.ID) error {
	if !idx.id.IsNull() {
		return errors.New("ID already set")
	}

	debug.Log("ID set to %v", id)
	idx.id = id

	return nil
}

// TreePacks returns a list of packs that contain only tree blobs.
func (idx *IndexBolt) TreePacks() restic.IDs {
	return nil
}

// LoadIndexBoltFromIndex loads the index from an standard index.
func MakeIndexBoltFromIndex(ctx context.Context, repo restic.Repository, idx restic.FileIndex) *IndexBolt {

	indexBolt := NewIndexBolt()

	indexBolt.ctx = ctx
	// TODO: error handling
	indexBolt.id, _ = idx.ID()

	db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket(indexBolt.IdToBytes())
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		for blob := range idx.Each(ctx) {
			newEntry := BoltindexEntry{
				PackID: blob.PackID,
				Offset: blob.Offset,
				Length: blob.Length,
			}
			h := restic.BlobHandle{ID: blob.ID, Type: blob.Type}
			err := b.Put(handleToBytes(h), indexEntryToBytes(newEntry))
			if err != nil {
				return err
			}
		}
		return nil
	})

	return indexBolt
}
