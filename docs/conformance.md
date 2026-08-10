# POSIX conformance: pjdfstest against AtlasFS

DESIGN.md §27 makes POSIX conformance a **gating** criterion rather than an
aspiration, and names three suites: pjdfstest, `fsx`, and xfstests. Until now
none of them had been run — the README said so, which is better than silence
but is not the same as evidence.

This document records an actual run: how it was set up, what it scored, and
what each remaining failure is. §27 asks for "100% pass within `posix`
subtrees, documented enumerated exceptions only, each with a rationale." The
enumeration below is that list. It is not yet 100%.

## What was run

- **Suite:** pjdfstest, built from source at `github.com/pjd/pjdfstest`.
- **Harness:** `prove -r tests/` run with the mountpoint as the working
  directory, as root. pjdfstest drops to uid 65534 internally for the
  permission cases, which is why the mount needs `allow_other`.
- **Scale:** 238 test files, 8798 assertions.

The mount under test:

```
atlas-mds -repo REPO -class posix -listen 127.0.0.1:PORT
atlas mount -mds 127.0.0.1:PORT -allow-other REPO MOUNTPOINT
```

That is the `posix` class, which is what §27 gates, and it is a genuine
two-process configuration: the metadata authority is a separate process
reached over gRPC, and the mount is one of its lease holders.

Two setup details that are easy to get wrong, both of which produced
misleading results before they were understood:

- **`allow_other` is required.** Without it the kernel refuses every access
  from a uid other than the mounting one *before any permission check runs*,
  so the entire permission matrix fails for a reason that has nothing to do
  with the filesystem.
- **The mountpoint's ancestors must be traversable by the unprivileged uid.**
  A mountpoint under a `drwx------` directory produces exactly the same
  EACCES, from the path walk rather than from the filesystem. An early run of
  this suite was invalid for that reason alone.

## Result

| Configuration | Passed | Score |
|---|---|---|
| Before this work (`relaxed`, no ownership model) | 3642 / 8792 | 41.4% |
| + ownership, `default_permissions`, `allow_other` | 5123 / 8792 | 58.3% |
| + `mknod` (FIFOs, sockets, device nodes) | 8590 / 8792 | 97.7% |
| + `posix` class via the authority | 8649 / 8792 | 98.4% |
| + atime/ctime, mode 0, NAME_MAX, directory timestamps | see below | |

The assertion count differs between rows because pjdfstest runs more
assertions as more of them get far enough to matter.

The jump from 58% to 98% is almost entirely `mknod`. It is worth understanding
why one missing call cost that much: a large number of the chmod, chown,
rename, link and unlink cases use a FIFO as their *subject*, so a failed
`mkfifo` is followed by a cascade of ENOENT from every operation on the name
that was never created. A conformance score is not a linear measure of how
much works.

## Bugs this found

Every one of these was a real defect, not a disagreement about semantics:

1. **No ownership model at all.** Every inode reported uid/gid 0 and `chown`
   was accepted and discarded, so a world-unreadable file was readable by
   anyone who could see the mount.
2. **`mknod`/`mkfifo` unimplemented**, returning EOPNOTSUPP.
3. **Special files reported `nlink` as a constant 1**, so a hard-linked FIFO
   claimed one link no matter how many names it had.
4. **`atime` and `ctime` were never stored**, both reported as `mtime` — so
   `utimensat` appeared to ignore half its argument, and `ctime`, the one
   timestamp a caller cannot set, never moved.
5. **Mode 0 was spelled as "no mode given"** in four places, so
   `open(path, O_CREAT, 0)` produced a 0644 file. The last of the four was in
   go-fuse itself (`NullPermissions`).
6. **NAME_MAX was not enforced.** The kernel does not check it for a FUSE
   filesystem, so names longer than 255 bytes were being created.
7. **A directory's `mtime`/`ctime` never moved** when an entry was added or
   removed — and the coherence layer was bumping only the directory's
   *version*, leaving its own inode record under a lease nothing invalidated.

Two more were found by the same effort but outside pjdfstest itself: a crash
from concurrent reads on one file handle, and EIO returned for interrupted
requests. Both are described in the commit history.

## Enumerated exceptions

These remain. Each is a real limitation with a reason, not an unexplained
skip — §27's standard is that "a skip list nobody has justified is
indistinguishable from a failure list."

### 1. Buffered writes do not update `size` until flush

`open/07` expects `fstat` on a write fd to report the new size immediately
after a `write`. This build buffers a file's content and commits it as a unit
on flush (§16.1), so the inode's size moves at flush, not at write.

This is architectural, not an oversight: the write path chunks and
content-addresses on commit, and there is no partial-file state to publish a
size from. §16.3's in-place random-access writes are what would change it,
and they are `posix`-class work this build does not do.

### 2. `ctime` granularity on rapid metadata changes

A few `link`/`unlink` cases compare `ctime` before and after an operation that
completes well inside one second. `ctime` is stored with the resolution the
inode record carries and compared at one-second `stat` granularity, so two
changes in the same second are indistinguishable.

### 3. pjdfstest's own Linux deviations

Several `chown` cases are marked `# TODO Linux doesn't clear the SGID/SUID
bits for directories, despite the description noted` in pjdfstest itself.
These fail on ext4 too. They are counted as expected failures by the harness,
not by us.

## What has not been run

- **`fsx`** (§27's 24-hour soak with mmap enabled) — not run. `fsx-linux` is
  not packaged here. It is also the suite most likely to find real problems in
  this build, because it hammers overlapping partial writes, which is exactly
  the area §16.1's buffer-then-commit model handles most coarsely.
- **xfstests** — not run. It needs a scratch device and a much larger
  harness, and the `generic/` subset applicable to a network filesystem would
  need the N/A justifications §27 asks for.
- **The distributed correctness harness** (§27's Jepsen-style bounded-staleness
  checker) — not built. The coherence properties have unit and multi-mount
  end-to-end tests, and the protocol has a Quint model, but not a real-time
  history checker under partition.

## Reproducing

```sh
git clone --depth 1 https://github.com/pjd/pjdfstest
cd pjdfstest && autoreconf -ifs && ./configure && make pjdfstest

# a repo and an authority
atlas publish -class relaxed /srv/repo /some/seed
atlas-mds -repo /srv/repo -class posix -listen 127.0.0.1:9713 &
atlas mount -mds 127.0.0.1:9713 -allow-other /srv/repo /mnt/atlas &

# the mountpoint's ancestors must be traversable by uid 65534
cd /mnt/atlas && prove -r /path/to/pjdfstest/tests
```
