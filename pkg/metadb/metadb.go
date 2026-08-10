// Package metadb is a single-node, embedded stand-in for the per-region
// FoundationDB metadata cluster in DESIGN.md §6/§7. It implements the
// same keyspace shape (dentries, inode records, global chunk locators)
// over bbolt so the rest of the system — packer, publisher, FUSE layer —
// is written against the real design's data model. Swapping in FDB later
// means replacing this package's internals, not its callers.
//
// A repo's consistency class (DESIGN.md §8) is stored once, at creation,
// and never changes for that repo's lifetime — this build's honest
// simplification of "per-subtree" down to "per-repo", since there is no
// subtree-boundary tracking here (one repo, one mount, one class). The
// `posix` class (real locking, blocking recall, O_APPEND) is not
// implemented; that needs a remote-holder recall protocol this
// single-process build has no second holder to exercise (see
// pkg/coherence's doc comment). `immutable`, `relaxed`, and `session`
// are.
//
// Quotas (DESIGN.md §18.3) are per-subtree in the general design, but
// this build has exactly one subtree per repo (no subtree-boundary
// tracking, same simplification as the consistency class above), so
// there is a single root-scope quota record rather than one keyed by
// subtree_id. The commit paths (CommitFile, CommitMkdir) charge it in
// the same bbolt transaction as the inode/dentry mutation they guard —
// the single-node stand-in for §18.3's "the counter can be updated in
// the same FDB transaction as the mutation using an atomic add."
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
	bucketQuota   = []byte("quota")   // "root" -> gob(quotaRecord), see package doc
)

var ErrNotFound = errors.New("metadb: not found")
var ErrExists = errors.New("metadb: already exists")
var ErrNotDir = errors.New("metadb: not a directory")
var ErrIsDirectory = errors.New("metadb: is a directory")
var ErrQuotaExceeded = errors.New("metadb: quota exceeded")

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
		for _, b := range [][]byte{bucketDentry, bucketInode, bucketLocator, bucketMeta, bucketQuota} {
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

var metaKeyClass = []byte("class")

// EnsureClass persists class as the repo's consistency class if none is
// stored yet (a brand-new repo), or returns whatever class was already
// persisted otherwise — a repo's class is fixed at creation and this is
// the one place that can be true even when a caller passes a different
// default, so reopening an existing `relaxed` repo via a plain Open()
// call (which internally hints "immutable") still returns "relaxed".
func (db *DB) EnsureClass(class string) (string, error) {
	var result string
	err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if v := b.Get(metaKeyClass); v != nil {
			result = string(v)
			return nil
		}
		result = class
		return b.Put(metaKeyClass, []byte(class))
	})
	return result, err
}

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

func allocInodeTx(tx *bbolt.Tx) (InodeID, error) {
	n, err := tx.Bucket(bucketMeta).NextSequence()
	if err != nil {
		return 0, err
	}
	return InodeID(n), nil
}

func (db *DB) AllocInode() (InodeID, error) {
	var id InodeID
	err := db.bolt.Update(func(tx *bbolt.Tx) error {
		var err error
		id, err = allocInodeTx(tx)
		return err
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

// SetDentry binds name -> child within dir, overwriting any existing
// binding. This is the mutable-class counterpart to CreateDentry
// (DESIGN.md §16.1's write path, and rename-over-existing semantics):
// callers on the immutable path use CreateDentry and get ErrExists on a
// collision; callers on a mutable-class write path use SetDentry and
// get upsert semantics instead. Same not-a-directory check as
// CreateDentry.
func (db *DB) SetDentry(dir InodeID, name string, child InodeID) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		rec, err := getInodeTx(tx, dir)
		if err != nil {
			return err
		}
		if !rec.IsDir {
			return ErrNotDir
		}
		return setDentryTx(tx, dir, name, child)
	})
}

func setDentryTx(tx *bbolt.Tx, dir InodeID, name string, child InodeID) error {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], uint64(child))
	return tx.Bucket(bucketDentry).Put(dentryKey(dir, name), v[:])
}

// RemoveDentry unbinds name within dir. Returns ErrNotFound if no such
// binding exists. Does not touch the target inode record — this build
// has no reference-counted GC (DESIGN.md §19 is a later phase), so an
// unlinked inode's record and chunks simply become unreachable rather
// than being reclaimed; that is a real, stated gap, not a leak this
// build hides.
func (db *DB) RemoveDentry(dir InodeID, name string) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketDentry)
		k := dentryKey(dir, name)
		if b.Get(k) == nil {
			return ErrNotFound
		}
		return b.Delete(k)
	})
}

// RemoveEntry is RemoveDentry plus the quota release the write path
// needs: it unbinds name in dir and, in the same bbolt transaction,
// credits back the removed inode's charge (its stored Size in bytes,
// one inode) so quota usage tracks live content across unlink/rmdir
// rather than only ever growing (DESIGN.md §18.3). Like RemoveDentry,
// the inode record itself is left in place.
func (db *DB) RemoveEntry(dir InodeID, name string) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		id, err := lookupTx(tx, dir, name)
		if err != nil {
			return err
		}
		rec, err := getInodeTx(tx, id)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketDentry).Delete(dentryKey(dir, name)); err != nil {
			return err
		}
		return applyQuotaDeltaTx(tx, -int64(rec.Size), -1)
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

func lookupTx(tx *bbolt.Tx, dir InodeID, name string) (InodeID, error) {
	v := tx.Bucket(bucketDentry).Get(dentryKey(dir, name))
	if v == nil {
		return 0, ErrNotFound
	}
	return InodeID(binary.BigEndian.Uint64(v)), nil
}

func (db *DB) Lookup(dir InodeID, name string) (InodeID, error) {
	var id InodeID
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		var err error
		id, err = lookupTx(tx, dir, name)
		return err
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

// --- quotas (DESIGN.md §18.3) --------------------------------------------

// quotaKeyRoot is the sole key in bucketQuota — see the package doc
// comment on why this build has one quota scope rather than one per
// subtree_id.
var quotaKeyRoot = []byte("root")

type quotaRecord struct {
	BytesUsed, InodesUsed   uint64
	BytesLimit, InodesLimit uint64 // 0 means unlimited
}

func getQuotaTx(tx *bbolt.Tx) (quotaRecord, error) {
	var q quotaRecord
	v := tx.Bucket(bucketQuota).Get(quotaKeyRoot)
	if v == nil {
		return q, nil // no limits set yet: zero value is "unlimited, nothing used"
	}
	err := gob.NewDecoder(bytes.NewReader(v)).Decode(&q)
	return q, err
}

func putQuotaTx(tx *bbolt.Tx, q quotaRecord) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(q); err != nil {
		return err
	}
	return tx.Bucket(bucketQuota).Put(quotaKeyRoot, buf.Bytes())
}

// addDelta applies delta (which may be negative, e.g. an unlink or a
// shrinking overwrite) to u, clamping at zero rather than wrapping —
// defensive only: a correct caller never drives usage negative, but
// uint64 underflow on a bug here would silently corrupt the counter
// instead of erroring.
func addDelta(u uint64, delta int64) uint64 {
	if delta >= 0 {
		return u + uint64(delta)
	}
	dec := uint64(-delta)
	if dec > u {
		return 0
	}
	return u - dec
}

// checkQuota reports ErrQuotaExceeded if applying the deltas to q would
// cross a set (non-zero) limit. A negative delta (freeing usage) never
// rejects, regardless of limit.
func checkQuota(q quotaRecord, byteDelta, inodeDelta int64) error {
	if q.BytesLimit > 0 && byteDelta > 0 && addDelta(q.BytesUsed, byteDelta) > q.BytesLimit {
		return ErrQuotaExceeded
	}
	if q.InodesLimit > 0 && inodeDelta > 0 && addDelta(q.InodesUsed, inodeDelta) > q.InodesLimit {
		return ErrQuotaExceeded
	}
	return nil
}

// applyQuotaDeltaTx is the one place usage is ever mutated: check then
// write, both inside the caller's existing bbolt transaction. Every
// commit path below calls this in the same tx.Update as its
// inode/dentry write, which is what makes a quota rejection abort the
// whole transaction — DESIGN.md §18.3's "the counter can be updated in
// the same ... transaction as the mutation using an atomic add" — rather
// than a separate check-then-write that could persist the mutation the
// check meant to block.
func applyQuotaDeltaTx(tx *bbolt.Tx, byteDelta, inodeDelta int64) error {
	q, err := getQuotaTx(tx)
	if err != nil {
		return err
	}
	if err := checkQuota(q, byteDelta, inodeDelta); err != nil {
		return err
	}
	q.BytesUsed = addDelta(q.BytesUsed, byteDelta)
	q.InodesUsed = addDelta(q.InodesUsed, inodeDelta)
	return putQuotaTx(tx, q)
}

// SetQuotaLimits sets this repo's byte and inode limits. A zero limit
// means unlimited for that dimension.
func (db *DB) SetQuotaLimits(bytesLimit, inodesLimit uint64) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		q, err := getQuotaTx(tx)
		if err != nil {
			return err
		}
		q.BytesLimit = bytesLimit
		q.InodesLimit = inodesLimit
		return putQuotaTx(tx, q)
	})
}

// GetQuotaUsage reports current byte and inode usage.
func (db *DB) GetQuotaUsage() (bytesUsed, inodesUsed uint64, err error) {
	err = db.bolt.View(func(tx *bbolt.Tx) error {
		q, err := getQuotaTx(tx)
		if err != nil {
			return err
		}
		bytesUsed, inodesUsed = q.BytesUsed, q.InodesUsed
		return nil
	})
	return
}

// CheckQuota reports ErrQuotaExceeded if applying (byteDelta, inodeDelta)
// to current usage would cross a set limit, without changing anything.
// It is a cheap, non-authoritative early-out: a caller like
// WriteHandle.Commit uses it to reject an over-quota write before doing
// any real chunking/storage work, so a rejection leaves no dangling
// chunk locator behind. The actual guarantee against exceeding a limit
// comes from CommitFile/CommitMkdir's atomic check-and-update in the
// same transaction as their mutation, not from this pre-check.
func (db *DB) CheckQuota(byteDelta, inodeDelta int64) error {
	return db.bolt.View(func(tx *bbolt.Tx) error {
		q, err := getQuotaTx(tx)
		if err != nil {
			return err
		}
		return checkQuota(q, byteDelta, inodeDelta)
	})
}

// --- atomic commit paths (mutation + quota charge in one transaction) ---

// CommitFile atomically resolves name within dir to an inode — reusing
// the existing inode if name is already bound to a (non-directory) file,
// allocating a fresh one and binding a new dentry otherwise — writes rec
// as that inode's record, and charges the quota for the resulting byte
// and inode delta, all within one bbolt transaction. Returns
// ErrIsDirectory if name is already bound to a directory, ErrNotDir if
// dir is not a directory, and ErrQuotaExceeded (transaction aborted,
// nothing persisted) if applying rec would exceed a set limit.
func (db *DB) CommitFile(dir InodeID, name string, rec InodeRecord) (id InodeID, err error) {
	err = db.bolt.Update(func(tx *bbolt.Tx) error {
		dirRec, err := getInodeTx(tx, dir)
		if err != nil {
			return err
		}
		if !dirRec.IsDir {
			return ErrNotDir
		}

		existing, lookupErr := lookupTx(tx, dir, name)
		isNew := errors.Is(lookupErr, ErrNotFound)
		var oldSize uint64
		switch {
		case lookupErr == nil:
			existingRec, err := getInodeTx(tx, existing)
			if err != nil {
				return err
			}
			if existingRec.IsDir {
				return ErrIsDirectory
			}
			id = existing
			oldSize = existingRec.Size
		case isNew:
			newID, err := allocInodeTx(tx)
			if err != nil {
				return err
			}
			id = newID
		default:
			return lookupErr
		}

		inodeDelta := int64(0)
		if isNew {
			inodeDelta = 1
		}
		if err := applyQuotaDeltaTx(tx, int64(rec.Size)-int64(oldSize), inodeDelta); err != nil {
			return err
		}
		if err := putInode(tx, id, rec); err != nil {
			return err
		}
		if isNew {
			return setDentryTx(tx, dir, name, id)
		}
		return nil
	})
	return id, err
}

// CommitMkdir atomically binds a new directory inode at (dir, name) and
// charges the quota for one inode — lookup, alloc, quota check, inode
// write, and dentry bind all happen in the same bbolt transaction, so a
// quota rejection aborts cleanly: no orphan inode, no dangling dentry.
// Returns ErrExists if name is already bound to anything, ErrNotDir if
// dir is not a directory, and ErrQuotaExceeded (nothing persisted) if
// the new inode would exceed a set inode limit.
func (db *DB) CommitMkdir(dir InodeID, name string, rec InodeRecord) (InodeID, error) {
	var id InodeID
	err := db.bolt.Update(func(tx *bbolt.Tx) error {
		dirRec, err := getInodeTx(tx, dir)
		if err != nil {
			return err
		}
		if !dirRec.IsDir {
			return ErrNotDir
		}
		if _, err := lookupTx(tx, dir, name); err == nil {
			return ErrExists
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		newID, err := allocInodeTx(tx)
		if err != nil {
			return err
		}
		if err := applyQuotaDeltaTx(tx, 0, 1); err != nil {
			return err
		}
		if err := putInode(tx, newID, rec); err != nil {
			return err
		}
		if err := setDentryTx(tx, dir, name, newID); err != nil {
			return err
		}
		id = newID
		return nil
	})
	return id, err
}
