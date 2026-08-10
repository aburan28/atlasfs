package metadb

import (
	"time"

	"go.etcd.io/bbolt"
)

// setattr(2)'s non-size half. Size is content, and changing it goes
// through the write path (chunk, upload, commit); mode and timestamps
// are pure metadata and change in one transaction here.
//
// Ownership (DESIGN.md §20) is here too. Permission *enforcement* is
// not: the mounts pass `default_permissions` so the kernel checks the
// mode/uid/gid this store reports, which is both less code and less
// likely to be subtly wrong than re-deriving POSIX access rules in
// userspace.

// AttrMutation names the attributes to change. A nil field is left
// alone, which is what setattr's valid-mask means: the kernel sends only
// the fields the caller actually set.
type AttrMutation struct {
	Mode  *uint32
	Uid   *uint32
	Gid   *uint32
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
		if mut.Uid != nil {
			rec.Uid = *mut.Uid
		}
		if mut.Gid != nil {
			rec.Gid = *mut.Gid
		}
		if mut.MTime != nil {
			rec.MTime = *mut.MTime
		}
		// chown(2) clears set-user-ID and set-group-ID on a successful
		// change by a non-root caller, and clearing them unconditionally
		// is the safe direction: leaving a setuid bit attached to a file
		// whose owner just changed is a privilege-escalation primitive.
		if (mut.Uid != nil || mut.Gid != nil) && !rec.IsDir {
			rec.Mode &^= 0o6000
		}
		if err := putInode(tx, id, rec); err != nil {
			return err
		}
		out = rec
		return nil
	})
	return out, err
}
