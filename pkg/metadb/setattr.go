package metadb

import (
	"time"

	"go.etcd.io/bbolt"
)

// setattr(2)'s non-size half. Size is content, and changing it goes
// through the write path (chunk, upload, commit); mode and timestamps
// are pure metadata and change in one transaction here.
//
// Scope, stated rather than implied: uid/gid are not stored. DESIGN.md
// §20's ownership and permission model is a later phase, and inventing
// an owner field ahead of it would mean a second source of truth to
// reconcile when the real one lands. chown is still accepted and
// ignored by the mounts, which is what most FUSE filesystems without an
// ownership model do — returning an error there breaks cp, rsync and
// tar, all of which try to restore ownership unconditionally.

// AttrMutation names the attributes to change. A nil field is left
// alone, which is what setattr's valid-mask means: the kernel sends only
// the fields the caller actually set.
type AttrMutation struct {
	Mode  *uint32
	MTime *time.Time
}

// SetAttr applies mut to id and returns the updated record.
func (db *DB) SetAttr(id InodeID, mut AttrMutation) (InodeRecord, error) {
	var out InodeRecord
	err := db.bolt.Update(func(tx *bbolt.Tx) error {
		rec, err := getInodeTx(tx, id)
		if err != nil {
			return err
		}
		if mut.Mode != nil {
			// Permission bits only: the type bits live in IsDir/IsSymlink,
			// and letting a chmod rewrite them would let a caller turn a
			// directory into a file by arithmetic.
			rec.Mode = *mut.Mode & 0o7777
		}
		if mut.MTime != nil {
			rec.MTime = *mut.MTime
		}
		if err := putInode(tx, id, rec); err != nil {
			return err
		}
		out = rec
		return nil
	})
	return out, err
}
