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
// subtree-boundary tracking here (one repo, one mount, one class).
//
// The `posix` class's blocking recall is implemented, but not through
// this file: it lives in pkg/coherence and is driven by pkg/mds, whose
// remote holders are what make a recall mean anything. What `posix`
// still lacks is byte-range locking (§17). A repo opened through
// pkg/repo therefore offers `immutable`, `relaxed`, and `session`; a
// `posix` subtree is served by cmd/atlas-mds.
//
// Quotas (DESIGN.md §18.3) are per-subtree in the general design, but
// this build has exactly one subtree per repo (no subtree-boundary
// tracking, same simplification as the consistency class above), so
// there is a single root-scope quota record rather than one keyed by
// subtree_id. The commit paths (CommitFile, CommitMkdir) charge it in
// the same bbolt transaction as the inode/dentry mutation they guard —
// the single-node stand-in for §18.3's "the counter can be updated in
// the same FDB transaction as the mutation using an atomic add."
//
// The graveyard (DESIGN.md §19.3) is this build's stand-in for nlink
// reaching zero: this build has no hardlinks, so RemoveEntry's dentry
// removal always is the last reference, and it records a
// (deleteTsNano||inodeID) graveyard entry rather than deleting the
// inode record outright. pkg/repo.Sweep is the mark-and-sweep
// implementation (DESIGN.md §19.1) that later reclaims a graveyard
// entry's chunks once it is past the grace period and deletes the
// entry and inode record; metadb itself only records and enumerates.
package metadb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
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
	bucketDentry  = []byte("dentry")    // (dirInode||0x00||name) -> childInode
	bucketInode   = []byte("inode")     // inodeID -> gob(InodeRecord)
	bucketLocator = []byte("locator")   // (region||0x00||chunkID) -> gob(pack.Locator)
	bucketMeta    = []byte("meta")      // "next_inode" -> uint64
	bucketQuota   = []byte("quota")     // "root" -> gob(quotaRecord), see package doc
	bucketGravey  = []byte("graveyard") // (deleteTsNano||inodeID) -> empty, see package doc
	bucketDeadCnt = []byte("deadcontainer")
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
	IsDir bool
	Mode  uint32
	// Uid and Gid are DESIGN.md §20's ownership, stored per inode.
	// They are the caller's at create time and change only through
	// chown; a zero pair means "unset" for records written before
	// ownership was tracked, and the mounts present that as root-owned,
	// which is what an unowned inode already looked like.
	Uid         uint32
	Gid         uint32
	Size        uint64
	MTime       time.Time
	NLink       uint32
	HasManifest bool
	ManifestID  manifest.ID
	HasInline   bool
	InlineChunk chunk.ID

	// Type holds the S_IFMT bits for a special file — FIFO, socket, or
	// device node — created by mknod(2). Zero means an ordinary file,
	// which is what every record written before special files existed
	// reads back as. Directories and symlinks keep their own flags
	// rather than moving here, so nothing about them changes.
	//
	// Rdev is the device number, meaningful only for S_IFCHR/S_IFBLK.
	Type uint32
	Rdev uint32

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
		for _, b := range [][]byte{bucketDentry, bucketInode, bucketLocator, bucketMeta, bucketQuota, bucketGravey, bucketDeadCnt} {
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
		// The root belongs to whoever created the repo. Leaving it
		// owned by uid 0 would make every mount by an ordinary user
		// read-only in practice: with `default_permissions` the kernel
		// refuses a create in a root-owned 0755 directory, so the first
		// write at the mount root fails with EACCES.
		if err := putInode(tx, RootInode, InodeRecord{
			IsDir: true, Mode: 0o755,
			Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid()),
			MTime: time.Now(), NLink: 2,
		}); err != nil {
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
// binding exists. Does not touch the target inode record or graveyard —
// callers that need the full unlink semantics (quota credit + graveyard
// entry) use RemoveEntry instead. Kept only for callers that genuinely
// just want to unbind a name without those side effects.
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

// RemoveEntry unbinds name in dir and, in the same bbolt transaction:
// credits back the removed inode's quota charge (its stored Size in
// bytes, one inode — DESIGN.md §18.3) and moves the inode into the
// graveyard at deletedAt rather than deleting its record outright
// (DESIGN.md §19.3 — this build has no hardlinks, so the one dentry
// binding removed here is always the last reference). deletedAt is
// supplied by the caller rather than computed here, matching how the
// rest of this package's timestamps (InodeRecord.MTime) are always
// caller-supplied — see pkg/repo.Repo.Clock.
func (db *DB) RemoveEntry(dir InodeID, name string, deletedAt time.Time) error {
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
		return dropLinkTx(tx, id, rec, deletedAt)
	})
}

// dropLinkTx removes one reference to an inode: nlink falls by one, and
// only when it reaches zero does the inode go to the graveyard and give
// its quota back (DESIGN.md §19.3 — nlink is updated transactionally
// with link/unlink, and the graveyard is what a zero-link inode enters
// instead of being deleted outright).
//
// Graving an inode that still has another name bound to it would make
// GC reclaim chunks the surviving link still reads, so the count is not
// bookkeeping — it is what keeps a hard-linked file readable after one
// of its names is removed.
func dropLinkTx(tx *bbolt.Tx, id InodeID, rec InodeRecord, deletedAt time.Time) error {
	// A directory's NLink is 2 by convention ("." plus its parent's
	// entry), not a hard-link count — Link refuses directories, so that 2
	// can never mean two names. Decrementing it here would leave every
	// rmdir'd directory un-graved and its inode unreclaimable.
	if !rec.IsDir && rec.NLink > 1 {
		rec.NLink--
		return putInode(tx, id, rec)
	}
	if err := applyQuotaDeltaTx(tx, -int64(rec.Size), -1); err != nil {
		return err
	}
	return addToGraveyardTx(tx, id, deletedAt)
}

// Link binds an additional name to an existing inode (§19.3). It is
// refused for directories — POSIX reserves directory hard links to the
// kernel's own "." and ".." — and it charges no quota: a link creates a
// dentry, not an inode, and the bytes were already counted once.
func (db *DB) Link(dir InodeID, name string, target InodeID) (InodeRecord, error) {
	var out InodeRecord
	err := db.bolt.Update(func(tx *bbolt.Tx) error {
		rec, err := getInodeTx(tx, target)
		if err != nil {
			return err
		}
		if rec.IsDir {
			return ErrIsDirectory
		}
		if dirRec, err := getInodeTx(tx, dir); err != nil {
			return err
		} else if !dirRec.IsDir {
			return ErrNotDir
		}
		if _, err := lookupTx(tx, dir, name); err == nil {
			return ErrExists
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		if rec.NLink == 0 {
			// Records written before nlink was tracked read back as 0;
			// treating that as one existing link keeps the count honest
			// rather than letting the first link make it 1.
			rec.NLink = 1
		}
		rec.NLink++
		if err := putInode(tx, target, rec); err != nil {
			return err
		}
		if err := setDentryTx(tx, dir, name, target); err != nil {
			return err
		}
		out = rec
		return nil
	})
	return out, err
}

// Rename moves the binding at (oldDir, oldName) to (newDir, newName),
// atomically. POSIX requires the whole thing to be one step — an
// observer must never see the name at neither location nor at both — so
// it is a single bbolt transaction rather than a remove and a create.
//
// Semantics implemented, and the reasoning where POSIX allows choices:
//
//   - Renaming onto an existing *file* replaces it, and the replaced
//     inode goes to the graveyard rather than being deleted, exactly as
//     unlink does (§19.3) — GC is what eventually reclaims it, and its
//     quota is released here.
//   - Renaming onto an existing *directory* requires that directory to
//     be empty, and renaming a non-directory onto a directory (or the
//     reverse) is refused. Those are POSIX's rules, and the emptiness
//     check has to happen inside this transaction or it races a create.
//   - Renaming a directory into its own subtree would detach that
//     subtree from the root, so it is refused with ErrInvalidRename.
//     Nothing else in the system would notice — the entries would simply
//     become unreachable and GC would eventually eat them.
//
// Quota is unchanged for a plain move: the same bytes and inode stay
// bound, just under a different name.
func (db *DB) Rename(oldDir InodeID, oldName string, newDir InodeID, newName string, deletedAt time.Time) error {
	if oldDir == newDir && oldName == newName {
		return nil
	}
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		srcID, err := lookupTx(tx, oldDir, oldName)
		if err != nil {
			return err
		}
		srcRec, err := getInodeTx(tx, srcID)
		if err != nil {
			return err
		}
		if newDirRec, err := getInodeTx(tx, newDir); err != nil {
			return err
		} else if !newDirRec.IsDir {
			return ErrNotDir
		}
		if srcRec.IsDir {
			if err := checkNotDescendantTx(tx, srcID, newDir); err != nil {
				return err
			}
		}

		if dstID, err := lookupTx(tx, newDir, newName); err == nil {
			dstRec, err := getInodeTx(tx, dstID)
			if err != nil {
				return err
			}
			switch {
			case dstRec.IsDir && !srcRec.IsDir:
				return ErrIsDirectory
			case !dstRec.IsDir && srcRec.IsDir:
				return ErrNotDir
			case dstRec.IsDir && srcRec.IsDir:
				kids, err := readdirTx(tx, dstID)
				if err != nil {
					return err
				}
				if len(kids) > 0 {
					return ErrNotEmpty
				}
			}
			// Displace the target: exactly the treatment an explicit
			// unlink would give, nlink included — a displaced name that
			// was one of several hard links must not grave the inode the
			// other links still read.
			if err := dropLinkTx(tx, dstID, dstRec, deletedAt); err != nil {
				return err
			}
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}

		if err := tx.Bucket(bucketDentry).Delete(dentryKey(oldDir, oldName)); err != nil {
			return err
		}
		return setDentryTx(tx, newDir, newName, srcID)
	})
}

// ErrNotEmpty is returned when an operation requires an empty directory.
var ErrNotEmpty = errors.New("metadb: directory not empty")

// ErrInvalidRename is returned for a rename that would detach a subtree
// from the root by moving a directory inside itself.
var ErrInvalidRename = errors.New("metadb: cannot rename a directory into its own subtree")

// checkNotDescendantTx walks up from start to the root, refusing if it
// meets ancestor. Walking up is O(depth); walking down from ancestor
// would be O(subtree), and depth is the smaller number by a wide margin
// for the trees this filesystem targets.
func checkNotDescendantTx(tx *bbolt.Tx, ancestor, start InodeID) error {
	for cur := start; cur != RootInode; {
		if cur == ancestor {
			return ErrInvalidRename
		}
		parent, err := parentOfTx(tx, cur)
		if err != nil {
			// No parent link found: treat as reaching the top rather than
			// failing the rename, since an unparented inode cannot be
			// inside ancestor's subtree either.
			return nil
		}
		if parent == cur {
			return nil
		}
		cur = parent
	}
	return nil
}

// parentOfTx finds a directory's parent by scanning dentries for the one
// binding that points at it.
//
// This is a scan because the schema has no parent pointer: DESIGN.md
// §6's inode record is attrs plus a content pointer, and adding a parent
// field would be a second place for the truth to live. Rename is rare
// and directory depth is small, so paying a scan here is cheaper than
// maintaining a denormalized link on every mutation.
func parentOfTx(tx *bbolt.Tx, child InodeID) (InodeID, error) {
	c := tx.Bucket(bucketDentry).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if InodeID(binary.BigEndian.Uint64(v)) == child {
			return InodeID(binary.BigEndian.Uint64(k[:8])), nil
		}
	}
	return 0, ErrNotFound
}

// CreateSpecial binds a new special file — FIFO, socket, or device node
// — at (dir, name). It has no content: a FIFO's data lives in the
// kernel's pipe buffer and a socket's in the network stack, so there is
// nothing to chunk. What the filesystem stores is the type, so lookup
// and getattr report it and the VFS handles the rest itself.
func (db *DB) CreateSpecial(dir InodeID, name string, typ uint32, rdev uint32, mode, uid, gid uint32) (InodeID, error) {
	rec := InodeRecord{
		Mode:  mode & 0o7777,
		Type:  typ,
		Rdev:  rdev,
		Uid:   uid,
		Gid:   gid,
		MTime: time.Now(),
		NLink: 1,
	}
	return db.PublishFile(dir, name, rec)
}

// CreateSymlink binds a new symlink at (dir, name). A symlink's target
// is stored verbatim in the inode record rather than chunked — its
// "content" is a short string, the same way a real filesystem treats a
// fast symlink (see InodeRecord.SymlinkTarget).
func (db *DB) CreateSymlink(dir InodeID, name, target string, uid, gid uint32) (InodeID, error) {
	rec := InodeRecord{
		Mode:          0o777,
		Uid:           uid,
		Gid:           gid,
		Size:          uint64(len(target)),
		MTime:         time.Now(),
		NLink:         1,
		IsSymlink:     true,
		SymlinkTarget: target,
	}
	return db.PublishFile(dir, name, rec)
}

// DeleteInode removes id's record outright. The only caller is
// pkg/repo.Sweep, and only for a graveyard entry already past DESIGN.md
// §19.2's grace period — by then nothing can still resolve to id via a
// dentry (RemoveEntry already removed the only one) or via a
// not-yet-expired graveyard entry (Sweep's mark phase treats those as
// reachable and never sweeps them).
func (db *DB) DeleteInode(id InodeID) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketInode).Delete(inodeKey(id))
	})
}

// DeleteLocator removes a chunk's locator entry for region. Deleting an
// already-absent key is not an error (bbolt's own Delete semantics),
// which matters here: a Sweep run interrupted after removing some
// locators but before removing the graveyard entry must be safe to
// retry against the same, now-partially-swept entry.
func (db *DB) DeleteLocator(region string, id chunk.ID) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketLocator).Delete(locatorKey(region, id))
	})
}

// --- graveyard (DESIGN.md §19.3) -----------------------------------------

// GraveyardEntry records an inode whose one dentry binding was removed,
// and when. DESIGN.md §19.1's mark phase treats a graveyard entry as
// reachable until DeletedAt+T_grace, so a chunk it alone references
// survives until then.
type GraveyardEntry struct {
	InodeID   InodeID
	DeletedAt time.Time
}

// graveyardKey is (deleteTsNano||inodeID) big-endian, so bbolt's
// lexicographic key order is also oldest-deletion-first — ListGraveyard
// relies on this rather than sorting after the fact.
func graveyardKey(deletedAt time.Time, id InodeID) []byte {
	var k [16]byte
	binary.BigEndian.PutUint64(k[:8], uint64(deletedAt.UnixNano()))
	binary.BigEndian.PutUint64(k[8:], uint64(id))
	return k[:]
}

func addToGraveyardTx(tx *bbolt.Tx, id InodeID, deletedAt time.Time) error {
	return tx.Bucket(bucketGravey).Put(graveyardKey(deletedAt, id), []byte{})
}

// ListGraveyard returns every graveyard entry, oldest deletion first.
func (db *DB) ListGraveyard() ([]GraveyardEntry, error) {
	var out []GraveyardEntry
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketGravey).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			ts := int64(binary.BigEndian.Uint64(k[:8]))
			id := InodeID(binary.BigEndian.Uint64(k[8:]))
			out = append(out, GraveyardEntry{InodeID: id, DeletedAt: time.Unix(0, ts)})
		}
		return nil
	})
	return out, err
}

// RemoveGraveyardEntry deletes e's graveyard record. The only caller is
// pkg/repo.Sweep, once e is past grace and its chunks have been
// evaluated (DESIGN.md §19.1's sweep step).
func (db *DB) RemoveGraveyardEntry(e GraveyardEntry) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketGravey).Delete(graveyardKey(e.DeletedAt, e.InodeID))
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
		var err error
		out, err = readdirTx(tx, dir)
		return err
	})
	return out, err
}

func readdirTx(tx *bbolt.Tx, dir InodeID) ([]DirEntry, error) {
	var out []DirEntry
	c := tx.Bucket(bucketDentry).Cursor()
	prefix := dentryPrefix(dir)
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		out = append(out, DirEntry{
			Name:  string(k[len(prefix):]),
			Inode: InodeID(binary.BigEndian.Uint64(v)),
		})
	}
	return out, nil
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
			if err := db.PutInode(id, InodeRecord{
				IsDir: true, Mode: 0o755,
				Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid()),
				MTime: time.Now(), NLink: 2,
			}); err != nil {
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

// LocatorEntry pairs a chunk with where it currently lives, for callers
// that need to reason about containers rather than individual chunks.
type LocatorEntry struct {
	ChunkID chunk.ID
	Locator pack.Locator
}

// ListLocators returns every locator bound in region. Compaction
// (DESIGN.md §19.1 step 3) needs this because liveness is a property of
// a *container* — "which chunks does this container still hold that
// anyone references" is not answerable from a per-chunk lookup.
func (db *DB) ListLocators(region string) ([]LocatorEntry, error) {
	prefix := append([]byte(region), 0x00)
	var out []LocatorEntry
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketLocator).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var e LocatorEntry
			copy(e.ChunkID[:], k[len(prefix):])
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&e.Locator); err != nil {
				return err
			}
			out = append(out, e)
		}
		return nil
	})
	return out, err
}

// DeadContainer is a container object that compaction has rewritten and
// which is therefore no longer referenced by any locator — but which is
// not deleted immediately. See MarkContainerDead.
type DeadContainer struct {
	Key       string
	RetiredAt time.Time
}

// MarkContainerDead records that key's contents have been rewritten
// elsewhere and it may be deleted from the backend once past grace.
//
// Compaction deliberately does not delete the old object inline. A
// reader that resolved a locator just before the rewrite is still
// holding the old (container, offset) pair and may be mid-Get; deleting
// underneath it would turn a GC run into a read error. Deferring the
// delete behind the same T_grace the graveyard uses (DESIGN.md §19.2)
// costs only storage, and storage is exactly what that invariant already
// trades away for safety.
func (db *DB) MarkContainerDead(key string, at time.Time) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(at); err != nil {
		return err
	}
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketDeadCnt).Put([]byte(key), buf.Bytes())
	})
}

// ListDeadContainers returns every container awaiting deletion.
func (db *DB) ListDeadContainers() ([]DeadContainer, error) {
	var out []DeadContainer
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketDeadCnt).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			d := DeadContainer{Key: string(k)}
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&d.RetiredAt); err != nil {
				return err
			}
			out = append(out, d)
		}
		return nil
	})
	return out, err
}

// RemoveDeadContainer drops the bookkeeping entry for key, once the
// backend object itself has actually been deleted.
func (db *DB) RemoveDeadContainer(key string) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketDeadCnt).Delete([]byte(key))
	})
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

// PublishFile binds a brand-new file inode at (dir, name) and charges
// the quota for it, in one transaction — the publish-path counterpart to
// CommitFile.
//
// It differs from CommitFile in exactly one way, and deliberately:
// rebinding an existing name is ErrExists rather than an in-place
// overwrite, because DESIGN.md §8 says an `immutable` subtree is not
// rewritten in place. That is why this cannot simply call CommitFile.
//
// Publish used to allocate, write, and bind outside any quota
// transaction, which meant the primary ingest path consumed no quota at
// all: a repo could be filled past a set byte limit by publishing, and
// `atlas quota` would report 0 used (DESIGN.md §18.3, and §22.3's
// capacity-is-a-quota claim for CSI).
func (db *DB) PublishFile(dir InodeID, name string, rec InodeRecord) (id InodeID, err error) {
	err = db.bolt.Update(func(tx *bbolt.Tx) error {
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

		id, err = allocInodeTx(tx)
		if err != nil {
			return err
		}
		if err := applyQuotaDeltaTx(tx, int64(rec.Size), 1); err != nil {
			return err
		}
		if err := putInode(tx, id, rec); err != nil {
			return err
		}
		return setDentryTx(tx, dir, name, id)
	})
	return id, err
}

// EnsureDirCharged is EnsureDir with quota accounting: each directory it
// actually creates costs one inode, and each is created inside its own
// transaction alongside that charge. Directories already present cost
// nothing, so it stays idempotent — publish walks the same parent
// directories repeatedly and must not be charged twice for them.
func (db *DB) EnsureDirCharged(path []string) (InodeID, error) {
	cur := RootInode
	for _, name := range path {
		if name == "" {
			continue
		}
		id, err := db.Lookup(cur, name)
		if err == nil {
			cur = id
			continue
		}
		if !errors.Is(err, ErrNotFound) {
			return 0, err
		}
		rec := InodeRecord{
			IsDir: true, Mode: 0o755,
			Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid()),
			MTime: time.Now(), NLink: 2,
		}
		id, err = db.CommitMkdir(cur, name, rec)
		if errors.Is(err, ErrExists) {
			// Raced with another writer creating the same directory;
			// theirs is as good as ours.
			if id, err = db.Lookup(cur, name); err != nil {
				return 0, err
			}
		} else if err != nil {
			return 0, err
		}
		cur = id
	}
	return cur, nil
}

// GetQuotaLimits reports the configured limits; 0 means unlimited.
func (db *DB) GetQuotaLimits() (bytesLimit, inodesLimit uint64, err error) {
	err = db.bolt.View(func(tx *bbolt.Tx) error {
		q, err := getQuotaTx(tx)
		if err != nil {
			return err
		}
		bytesLimit, inodesLimit = q.BytesLimit, q.InodesLimit
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
			// The caller supplies content and mtime — not link count, not
			// permissions. Taking rec's NLink verbatim would reset a
			// hard-linked file's count to 1 on every overwrite, after
			// which removing one of its names would grave an inode the
			// other name still resolves to and GC would reclaim chunks a
			// live file reads. Taking its Mode verbatim would strip the
			// exec bit off any script the caller merely rewrote, which
			// is what write-then-run workflows notice immediately.
			if existingRec.NLink > 1 {
				rec.NLink = existingRec.NLink
			}
			if existingRec.Mode != 0 {
				rec.Mode = existingRec.Mode
			}
			// Ownership likewise: writing to a file you do not own must
			// not quietly transfer it to you.
			rec.Uid, rec.Gid = existingRec.Uid, existingRec.Gid
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
