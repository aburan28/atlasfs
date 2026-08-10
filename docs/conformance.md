# POSIX conformance: pjdfstest against AtlasFS

DESIGN.md §27 makes POSIX conformance a **gating** criterion rather than an
aspiration, and names three suites: pjdfstest, `fsx`, and xfstests. Until now
none of them had been run — the README said so, which is better than silence
but is not the same as evidence. This records runs of the first two.

This document records those runs: how they were set up, what they scored, and
what each remaining failure is. §27 asks for "100% pass within `posix`
subtrees, documented enumerated exceptions only, each with a rationale." The
enumeration below is that list — one entry, plus pjdfstest's own known Linux
deviations. It is not 100%, and the gap is a feature this build has never
claimed to have.

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
| + atime/ctime and mode 0 | 8731 / 8798 | 99.24% |
| + NAME_MAX and directory timestamps | 8788 / 8798 | 99.89% |
| + subsecond times, directory nlink, truncate bound | 8794 / 8798 | 99.95% |
| **+ in-flight size while a write is open** (found by `fsx`) | **8796 / 8798** | **99.98%** |

The assertion count differs between rows because pjdfstest runs more
assertions as more of them get far enough to matter.

The two remaining failures are both in `unlink/14`, the single exception
enumerated below. Nothing else in the suite fails.

The jump from 58% to 98% is almost entirely `mknod`. It is worth understanding
why one missing call cost that much: a large number of the chmod, chown,
rename, link and unlink cases use a FIFO as their *subject*, so a failed
`mkfifo` is followed by a cascade of ENOENT from every operation on the name
that was never created. A conformance score is not a linear measure of how
much works.

## Bugs this found

Every one of these was a real defect, not a disagreement about semantics.
The list is the argument for running the suite at all: none of them were
found by this project's own tests, and two are worse than conformance
failures.

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
6. **A directory's `mtime`/`ctime` never moved** when an entry was added or
   removed — and the coherence layer was bumping only the directory's
   *version*, leaving its own inode record under a lease nothing invalidated.
7. **NAME_MAX was not enforced.** The kernel does not check it for a FUSE
   filesystem, so names longer than 255 bytes were being created.
8. **A directory's `nlink` was a constant 2** rather than 2 plus its
   subdirectory count. `find` uses `nlink-2` to decide a directory has no
   subdirectories left to visit, so this could make it skip real subtrees.
9. **`truncate` had no upper bound.** The write path buffers a whole file in
    memory and sized that buffer from the requested length, so an
    unprivileged `truncate(f, 1<<62)` was a local denial of service. This one
    is a security bug, not a conformance detail.
10. **Timestamps were reported to the second**, discarding the nanoseconds
    the records already held.

Three more were found by the same effort but outside pjdfstest itself: a
crash from concurrent reads on one file handle (a Go fatal error, so the
whole mount died), EIO returned for interrupted requests, and — from the
fix for that one — a mutation being applied twice when the kernel resent an
interrupted request. All three are in the commit history.

## Enumerated exceptions

These remain. Each is a real limitation with a reason, not an unexplained
skip — §27's standard is that "a skip list nobody has justified is
indistinguishable from a failure list."

### 1. Open-but-unlinked files are not kept alive (`unlink/14`, 2 assertions)

POSIX requires an unlinked file to stay readable through an already-open
descriptor until the last one closes. This build graves an inode as soon as
its last *name* goes, without waiting for open handles — the gap
`pkg/repo/gc.go` has documented since GC was written, and §19.3's
leased-open-handle registry is what would close it. It needs open-handle
tracking across process boundaries, which is authority work rather than a
mount-local fix.

### 2. pjdfstest's own Linux deviations

Several `chown` cases are marked `# TODO Linux doesn't clear the SGID/SUID
bits for directories, despite the description noted` in pjdfstest itself.
These fail on ext4 too. They are counted as expected failures by the harness,
not by us.

## fsx

§27 also names `fsx`, and predicted correctly: it is the suite that finds
things pjdfstest cannot, because it hammers overlapping partial writes and
truncates against a shadow copy and compares byte for byte.

Built from xfstests' `ltp/fsx.c` (it needs a small stand-in for the
`config.h` that xfstests' configure generates; the reproduction steps below
include it).

It failed on its **third operation** the first time it ran: a write past EOF
left `stat` reporting the pre-write size, because content commits on flush
(§16.1) and nothing consulted the open write handle for the in-flight
length. That is the same defect pjdfstest's `open/07` reported through a much
smaller hole. With it fixed:

| Run | Operations | mmap | Result |
|---|---|---|---|
| authority-backed `posix` mount | 50,000 | yes | all A-OK |
| authority-backed, 3 seeds concurrently | 3 × 30,000 | yes | all A-OK |
| in-process `relaxed` mount | 30,000 | yes | all A-OK |

That is 170,000 operations with mmap reads and writes enabled, against both
mount implementations, with three of the runs concurrent against one mount.

`fsx` also reports what the filesystem does not support, which is a useful
inventory in itself: `FALLOC_FL_KEEP_SIZE`, `PUNCH_HOLE`, `ZERO_RANGE`,
`COLLAPSE_RANGE`, `INSERT_RANGE`, `UNSHARE_RANGE`, dontcache I/O, and
`O_DIRECT` (which atomic writes need). `fallocate` in general is
unimplemented; a content-addressed store has no preallocation to do, but
`PUNCH_HOLE` is a real gap for a sparse-file workload.

**This is not §27's bar.** §27 asks for a 24-hour soak; these runs take
minutes. What they establish is that the write path survives sustained
random overlapping I/O with mmap, not that it survives a day of it.

## What has not been run

- **`fsx` for 24 hours** — see above. The runs here are minutes, not a day.
- **xfstests** — not run. It needs a scratch device and a much larger
  harness, and the `generic/` subset applicable to a network filesystem would
  need the N/A justifications §27 asks for.
- **The distributed correctness harness** (§27's Jepsen-style bounded-staleness
  checker) — not built. The coherence properties have unit and multi-mount
  end-to-end tests, and the protocol has a Quint model, but not a real-time
  history checker under partition.

## Reproducing

### fsx

```sh
# fsx.c from xfstests, plus the two headers it includes
curl -O https://raw.githubusercontent.com/kdave/xfstests/master/ltp/fsx.c
curl -O https://raw.githubusercontent.com/kdave/xfstests/master/src/global.h
curl -O https://raw.githubusercontent.com/kdave/xfstests/master/src/statx.h
# global.h includes <config.h>, generated by xfstests' configure; a file
# defining HAVE_<HEADER>_H for the usual Linux headers plus HAVE_ERR_H,
# HAVE_LINUX_FALLOC_H, HAVE_COPY_FILE_RANGE and STDC_HEADERS is enough.
gcc -O2 -D_GNU_SOURCE -I. -o fsx fsx.c   # add #include <getopt.h>

cd /mnt/atlas && ./fsx -N 50000 -S 42 testfile
```

### pjdfstest

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
