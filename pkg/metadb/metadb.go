// Package metadb is a single-node, embedded stand-in for the per-region
// FoundationDB metadata cluster in DESIGN.md §6/§7. It implements the
// same keyspace shape (dentries, inode records, global chunk locators)
// over bbolt so the rest of the system — packer, publisher, FUSE layer —
// is written against the real design's data model. Swapping in FDB later
// means replacing this package's internals, not its callers.
//
// This build only needs the `immutable` consistency class (DESIGN.md
// §8), which requires no leases, no dirver bumps, and no locking — so
// none of those keyspace regions from §6 are implemented here. What's
// here is dentries, inode records, and locators, which is what §5-§7
// actually require to publish and read back a tree.
package metadb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/manifest"
	"github.com/aburan28/atlasfs/pkg/pack"
)

type InodeID uint64

// RootInode is the fixed inode number for a repo's root directory.
const RootInode InodeID = 1

var (
	bucketDentry  = []byte("dentry")  // (dirInode||0x00||name) -> childInode
	bucketInode   = []byte("inode")   // inodeID -> gob(InodeRecord)
	bucketLocator = []byte("locator") // (region||0x00||chunkID) -> gob(pack.Locator)
	bucketMeta    = []byte("meta")    // "next_inode" -> uint64
)

var ErrNotFound = errors.New("metadb: not found")
var ErrExists = errors.New("metadb: already exists")
var ErrNotDir = errors.New("metadb: not a directory")

// InodeRecord is the attrs+content-pointer record from DESIGN.md §7's
// inode layer. A file is either a manifest reference (multi-chunk) or an
// inline single chunk (§5.3: "small files ... inline the chunk ID
// directly in the inode and skip the manifest object entirely").
type InodeRecord struct {
	IsDir       bool
	Mode        uint32
	Size        uint64
	MTime       time.Time
	NLink       uint32
	HasManifest bool
	ManifestID  manifest.ID
	HasInline   bool
	InlineChunk chunk.ID

	// IsSymlink and SymlinkTarget hold a symlink's target path verbatim.
	// A symlink is never chunked or content-addressed — its "content"
	// is the target string, small and stored directly in the inode
	// record, the same way a real filesystem treats a fast symlink.
	IsSymlink     bool
	SymlinkTarget string
}

type DirEntry struct {
	Name  string
	Inode InodeID
}

type DB struct {
	bolt *bbolt.DB
}

func Open(path string) (*DB, error) {
	bdb, err := bbolt.Open(path, 0o644, nil)
	if err != nil {
		return nil, fmt.Errorf("metadb: open: %w", err)
	}
	err = bdb.Update(func(tx *bbolt.Tx) error {
		for _, b := range [][]byte{bucketDentry, bucketInode, bucketLocator, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		bdb.Close()
		return nil, err
	}
	db := &DB{bolt: bdb}
	if err := db.ensureRoot(); err != nil {
		bdb.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error { return db.bolt.Close() }

func (db *DB) ensureRoot() error {
	_, err := db.GetInode(RootInode)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		if err := putInode(tx, RootInode, InodeRecord{IsDir: true, Mode: 0o755, MTime: time.Now(), NLink: 2}); err != nil {
			return err
		}
		return advanceSeqTo(tx, uint64(RootInode))
	})
}

// --- inode allocation & records ---------------------------------------

func advanceSeqTo(tx *bbolt.Tx, n uint64) error {
	b := tx.Bucket(bucketMeta)
	cur := b.Sequence()
	if n > cur {
		return b.SetSequence(n)
	}
	return nil
}

func (db *DB) AllocInode() (InodeID, error) {
	var id InodeID
	err := db.bolt.Update(func(tx *bbolt.Tx) error {
		n, err := tx.Bucket(bucketMeta).NextSequence()
		if err != nil {
			return err
		}
		id = InodeID(n)
		return nil
	})
	return id, err
}

func putInode(tx *bbolt.Tx, id InodeID, rec InodeRecord) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(rec); err != nil {
		return err
	}
	return tx.Bucket(bucketInode).Put(inodeKey(id), buf.Bytes())
}

func inodeKey(id InodeID) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(id))
	return k[:]
}

func (db *DB) PutInode(id InodeID, rec InodeRecord) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error { return putInode(tx, id, rec) })
}

func (db *DB) GetInode(id InodeID) (InodeRecord, error) {
	var rec InodeRecord
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketInode).Get(inodeKey(id))
		if v == nil {
			return ErrNotFound
		}
		return gob.NewDecoder(bytes.NewReader(v)).Decode(&rec)
	})
	return rec, err
}

// --- dentries -----------------------------------------------------------

func dentryKey(dir InodeID, name string) []byte {
	buf := make([]byte, 8, 9+len(name))
	binary.BigEndian.PutUint64(buf, uint64(dir))
	buf = append(buf, 0x00)
	buf = append(buf, name...)
	return buf
}

func dentryPrefix(dir InodeID) []byte {
	buf := make([]byte, 9)
	binary.BigEndian.PutUint64(buf, uint64(dir))
	buf[8] = 0x00
	return buf
}

// CreateDentry binds name -> child within dir. Fails with ErrExists if
// the name is already bound (DESIGN.md's `immutable` class never
// rebinds a published name).
func (db *DB) CreateDentry(dir InodeID, name string, child InodeID) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		rec, err := getInodeTx(tx, dir)
		if err != nil {
			return err
		}
		if !rec.IsDir {
			return ErrNotDir
		}
		b := tx.Bucket(bucketDentry)
		k := dentryKey(dir, name)
		if b.Get(k) != nil {
			return ErrExists
		}
		var v [8]byte
		binary.BigEndian.PutUint64(v[:], uint64(child))
		return b.Put(k, v[:])
	})
}

func getInodeTx(tx *bbolt.Tx, id InodeID) (InodeRecord, error) {
	var rec InodeRecord
	v := tx.Bucket(bucketInode).Get(inodeKey(id))
	if v == nil {
		return rec, ErrNotFound
	}
	err := gob.NewDecoder(bytes.NewReader(v)).Decode(&rec)
	return rec, err
}

func (db *DB) Lookup(dir InodeID, name string) (InodeID, error) {
	var id InodeID
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketDentry).Get(dentryKey(dir, name))
		if v == nil {
			return ErrNotFound
		}
		id = InodeID(binary.BigEndian.Uint64(v))
		return nil
	})
	return id, err
}

// Readdir returns all children of dir in name order (bbolt keys are
// lexicographically ordered by construction, matching DESIGN.md §18.1's
// pagination-by-key approach — this just returns the whole page since a
// dev-scale repo never approaches the 10M-entry case that motivates
// cursor pagination there).
func (db *DB) Readdir(dir InodeID) ([]DirEntry, error) {
	var out []DirEntry
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketDentry).Cursor()
		prefix := dentryPrefix(dir)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			out = append(out, DirEntry{
				Name:  string(k[len(prefix):]),
				Inode: InodeID(binary.BigEndian.Uint64(v)),
			})
		}
		return nil
	})
	return out, err
}

// EnsureDir walks path from root, creating any missing directory inodes,
// and returns the leaf directory's inode.
func (db *DB) EnsureDir(path []string) (InodeID, error) {
	cur := RootInode
	for _, name := range path {
		if name == "" {
			continue
		}
		id, err := db.Lookup(cur, name)
		if errors.Is(err, ErrNotFound) {
			id, err = db.AllocInode()
			if err != nil {
				return 0, err
			}
			if err := db.PutInode(id, InodeRecord{IsDir: true, Mode: 0o755, MTime: time.Now(), NLink: 2}); err != nil {
				return 0, err
			}
			if err := db.CreateDentry(cur, name, id); err != nil && !errors.Is(err, ErrExists) {
				return 0, err
			}
		} else if err != nil {
			return 0, err
		}
		cur = id
	}
	return cur, nil
}

// Resolve walks a full "/"-rooted path to its inode and record.
func (db *DB) Resolve(path []string) (InodeID, InodeRecord, error) {
	cur := RootInode
	rec, err := db.GetInode(cur)
	if err != nil {
		return 0, InodeRecord{}, err
	}
	for _, name := range path {
		if name == "" {
			continue
		}
		id, err := db.Lookup(cur, name)
		if err != nil {
			return 0, InodeRecord{}, err
		}
		rec, err = db.GetInode(id)
		if err != nil {
			return 0, InodeRecord{}, err
		}
		cur = id
	}
	return cur, rec, nil
}

// --- locators (global, region-keyed per DESIGN.md §7.5) -----------------

func locatorKey(region string, id chunk.ID) []byte {
	buf := make([]byte, 0, len(region)+1+len(id))
	buf = append(buf, region...)
	buf = append(buf, 0x00)
	buf = append(buf, id[:]...)
	return buf
}

// PutLocator adds a (region -> location) binding for a chunk. This is
// the one write DESIGN.md §7.5 calls commutative and safe without a
// home-region authority: two writers racing to add the same binding are
// both correct.
func (db *DB) PutLocator(region string, id chunk.ID, loc pack.Locator) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(loc); err != nil {
		return err
	}
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketLocator).Put(locatorKey(region, id), buf.Bytes())
	})
}

func (db *DB) GetLocator(region string, id chunk.ID) (pack.Locator, bool, error) {
	var loc pack.Locator
	found := false
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketLocator).Get(locatorKey(region, id))
		if v == nil {
			return nil
		}
		found = true
		return gob.NewDecoder(bytes.NewReader(v)).Decode(&loc)
	})
	return loc, found, err
}

// HasLocator reports whether a chunk is already known in region, which
// is the write-path dedup check from DESIGN.md §16.1 step 2: "checked
// against local cache and the locator index, and uploaded only if
// absent."
func (db *DB) HasLocator(region string, id chunk.ID) (bool, error) {
	_, found, err := db.GetLocator(region, id)
	return found, err
}
