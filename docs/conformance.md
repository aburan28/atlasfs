# POSIX conformance: pjdfstest against AtlasFS

DESIGN.md §27 makes POSIX conformance a **gating** criterion rather than an
aspiration, and names three suites: pjdfstest, `fsx`, and xfstests. Until now
none of them had been run — the README said so, which is better than silence
but is not the same as evidence. This records runs of the first two.

This document records those runs: how they were set up and what they scored.
§27 asks for "100% pass within `posix` subtrees, documented enumerated
exceptions only, each with a rationale."

**pjdfstest now meets that bar exactly: 8798 / 8798, with no exceptions
left to enumerate.** That is one of §27's three suites. `fsx` and xfstests
are covered further down and neither is at §27's stated depth, so the
gating criterion as a whole is not met — see "What has not been run".

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
| + in-flight size while a write is open (found by `fsx`) | 8796 / 8798 | 99.98% |
| **+ open-but-unlinked: nlink 0, and no resurrection on flush** | **8798 / 8798** | **100%** |

The assertion count differs between rows because pjdfstest runs more
assertions as more of them get far enough to matter.

**Nothing in the suite fails.** `prove -r` reports `Files=238, Tests=8798,
Result: PASS` against a `posix`-class mount served by a separate
`atlas-mds` process. The only annotated lines are seven `TODO passed` in
`chown/00.t` — pjdfstest's own markers for Linux SGID/SUID behaviour it
expects to fail, which pass here; TAP counts an unexpected pass as a
pass, not a failure.

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

**There are none.** §27's standard is that "a skip list nobody has
justified is indistinguishable from a failure list", so the two entries
this section used to hold are kept below as history rather than deleted —
what they were, and what closing them actually took.

### 1. Open-but-unlinked files — closed

This listed `unlink/14` (2 assertions) as an accepted exception, on the
reasoning that keeping an unlinked inode alive needed cross-process
open-handle tracking and so was authority work. Reproducing it directly
showed three separate defects hiding behind that one sentence, and none
of them was the one described.

`fstat` on a descriptor held across the unlink reported `nlink`
1 instead of 0 — reads and writes through it already worked, because the
graveyard retains the inode record. The second gap was that a flush
*resurrected the name*: writes commit on close (§16.1), and the commit
bound (dir, name) unconditionally, so a file unlinked while open for
write reappeared in a directory the user had emptied. pjdfstest sees
that only as an rmdir returning ENOTEMPTY, several steps from the cause.

The third gap was not a conformance failure at all: GC could reclaim a
file that was still open. Grace does not cover it — Invariant GC-1 sizes
`T_grace` against `T_write_max`, the age of an uncommitted *write
session*, whereas a descriptor may stay open for as long as its process
lives. `Repo.OpenHandles` now pins an inode while any descriptor is open
on it, and both phases of the sweep honour the pin.

That registry is per-process, which is sufficient rather than a
simplification for the in-process mount: metadb's bbolt file takes an
exclusive inter-process lock, so no second process can open the repo
while a mount holds it and any `Sweep` necessarily runs inside the
mount's own process. Verified rather than assumed — a second opener
blocks and times out. §19.3's *leased* handles remain the answer for the
authority-backed mount, where nothing needs them yet because `pkg/mds`
exposes no sweep.

### 2. pjdfstest's own Linux deviations — no longer failing

Several `chown` cases are marked `# TODO Linux doesn't clear the SGID/SUID
bits for directories, despite the description noted` in pjdfstest itself,
and fail on ext4 too. They now *pass* here and are reported as `TODO
passed`, which TAP counts as a pass. Nothing is being suppressed: the
suite's overall result is `PASS` with zero failures.

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

Since then `fallocate` has been implemented on both mounts, so most of
that inventory is no longer a gap — see "fallocate" below.

`fsx` also reported what the filesystem did not support, which was a useful
inventory in itself: `FALLOC_FL_KEEP_SIZE`, `PUNCH_HOLE`, `ZERO_RANGE`,
`COLLAPSE_RANGE`, `INSERT_RANGE`, `UNSHARE_RANGE`, dontcache I/O, and
`O_DIRECT` (which atomic writes need).

### fallocate

`FALLOC_FL_KEEP_SIZE`, `PUNCH_HOLE` and `ZERO_RANGE` are now implemented
on both mounts, along with plain allocate. The write path buffers a
file's whole content and commits it as a unit (§16.1), so there is no
block allocator and "preallocate" has no meaning — every mode reduces to
arranging bytes in that buffer, and the observable contract of all four
is the same: which bytes read as zeros, and whether the length moves.

Two things that are worth being explicit about rather than letting a
caller discover:

- **`PUNCH_HOLE` does not save space.** Zeroing is the honest
  implementation here because there is no sparse representation to
  exploit, but reclaiming storage is usually the whole reason to punch a
  hole. Correct data, no reclaim.
- **`COLLAPSE_RANGE` and `INSERT_RANGE` return `EOPNOTSUPP`** rather than
  being faked. Both are defined on filesystem block boundaries and this
  build has no block size to expose — and a caller that gets `EOPNOTSUPP`
  can fall back, while one that gets success has silently lost the
  operation.

The semantics live in `pkg/repo` rather than in either mount, so the two
mounts cannot drift: `atlas mount` and `atlas mount -mds` share one
implementation.

`O_DIRECT` and dontcache remain unimplemented.

**This is not §27's bar.** §27 asks for a 24-hour soak; these runs take
minutes. What they establish is that the write path survives sustained
random overlapping I/O with mmap, not that it survives a day of it.

## xfstests

xfstests has first-class FUSE support (its own `README.fuse`), so this is a
supported configuration rather than a hack: `FSTYP=fuse`, `FUSE_SUBTYP`, and
a `/sbin/mount.fuse.atlasfs` helper that mount(8) invokes.

Getting there needed one product change and one one-line change to xfstests
itself, both disclosed rather than buried:

- **`atlas mount -fsname`.** mount(8) and `findmnt` identify a mount by the
  device string they were given, and both mounts hardcoded their source name,
  so xfstests could not find its own test filesystem. A mount helper has to
  be able to set it; that is now a flag.
- **One line in `common/rc`.** `_fs_type` maps `fuse.glusterfs` → `glusterfs`
  and `fuse.ceph-fuse` → `ceph-fuse` for out-of-tree FUSE filesystems.
  `fuse.atlasfs` → `fuse` is the same entry for a filesystem that is not
  upstream. Nothing else in xfstests was modified.

Run so far, against a `relaxed`-class in-process mount:

```
generic/001 generic/002 generic/005 generic/006 generic/007   — all pass
```

`generic/002` failed the first time, and it found a coherence bug that
DESIGN.md had already specified: §10.5 puts `nlink` in the *inode's* lease
domain — "bumped by ... link/unlink (via nlink)" — and `Unlink` bumped only
the directory, so a holder that reached the file by another name kept serving
the pre-unlink link count. The test creates twenty links and removes them one
at a time, which is exactly the shape that exposes it.

A `./check -g quick` run was started and stopped part-way (see below). Of the
49 tests it reached, 38 were `[not run]` — and xfstests states its own reason
for each, which is the N/A justification §27 asks for rather than a silent
skip:

| Reason | Count |
|---|---|
| `require non2 to be valid block disk` | 10 |
| `xfs_io fpunch failed` (no `FALLOC_FL_PUNCH_HOLE`) | 8 |
| `xfs_io fzero failed` (no `FALLOC_FL_ZERO_RANGE`) | 3 |
| `fuse does not support shutdown` | 3 |
| `attr` / `chacl` command not installed | 5 |
| `kernel doesn't support renameat2` | 2 |
| other | 7 |

The block-device ones are structurally N/A for a filesystem with no block
device. The eleven `fpunch`/`fzero` skips were the largest *closable*
group, and they are what motivated implementing `fallocate`: `generic/008`
was `[not run] xfs_io fpunch failed` and now runs and passes. `generic/009`
still does not run, but for an unrelated reason — `xfs_io fiemap failed`,
the extent-mapping ioctl, which is a separate gap.

**This is not §27's bar either.** §27 asks for "the `generic/` groups
applicable to a network filesystem", which is hundreds of tests. What ran
here is a handful plus a partial quick group.

## What the long fsx soak found

A one-hour `fsx --duration=3600` run was started and did **not** complete: it
stopped with `domapwrite: ftruncate: Input/output error` after filling the
volume. The repo had grown to **21 GB** for a file fsx keeps under 256 KB.

The first diagnosis written here was wrong, and the correction is the more
useful finding: this said the superseded chunks were merely waiting for a
GC nobody ran. **Running GC would not have reclaimed a single byte.**

`Sweep` only ever walked the graveyard, and an overwrite creates no
graveyard entry — the inode is repointed at new content and the old chunks
are left referenced by nothing at all. So superseded chunks were invisible
to both halves of mark-and-sweep and leaked permanently. Reproduced
directly: 25 overwrites of one file left 25 live locators, and a full GC
pass freed none of them (`TestRepeatedOverwritesDoNotGrowStorageWithoutBound`).
That is now fixed — `sweepOrphans` collects every chunk the mark phase
cannot reach, gated on its container's seal time so an in-flight write is
never mistaken for garbage.

The leak was the unbounded half. The other half is write amplification,
and it is a policy gap rather than a bug. A commit re-chunks the whole
file and stores whatever the locator index lacks, so the unit of
amplification is the chunk: at the 4 MiB default, **every rewrite of a
smaller file stores a complete new copy**. Measured over 200 rewrites of a
256 KiB file changing 4 KiB each time:

| chunk size | stored |
|---|---|
| 4 MiB (default) | 50.0 MiB |
| 64 KiB | 13.3 MiB |
| 16 KiB | 4.2 MiB |

fsx keeps its file under 256 KB and ran 170k operations, which lands
squarely on the 21 GB observed. DESIGN.md §14.3 already calls `chunk_size`
a per-subtree policy "with defaults chosen per workload"; it is now
persisted at creation and reachable as `atlas -chunk-size`, so a
rewrite-heavy subtree can be created with a chunk size below its typical
file size. It is fixed for the repo's lifetime because re-chunking the
same bytes at a different size changes every chunk ID and would silently
disable dedup.

Both halves were then measured end to end on a real mount, same seed,
same 20,000 fsx operations, both runs `All operations completed A-OK`:

| chunk size | backing store after 20,000 fsx ops |
|---|---|
| 4 MiB (default) | **2500 MiB** |
| 16 KiB (`-chunk-size 16384`) | **451 MiB** |

2500 MiB × (170,000 / 20,000) ≈ 21 GB, which is what the original soak
reached — the diagnosis reproduces to within the noise of a different
seed.

### The constraint that actually doomed the soak

Even with the leak fixed, the soak could not have been rescued by running
GC, because **GC cannot run while a mount is up**. metadb's bbolt file
takes an exclusive inter-process lock, so `atlas gc` on a mounted repo
blocks for as long as the mount holds it — verified: it hangs until
killed. A long-running mount therefore never reclaimed anything, by
construction.

`atlas mount -gc-interval` is the fix. Sweeping from inside the mount's
own process is also what makes it *correct* rather than merely possible:
`Repo.OpenHandles` lives there, so the sweep can see which inodes still
have descriptors open and skip them (§19.3). Verified under load — 15,000
fsx operations all A-OK with a sweep firing every 20 s against the same
live repo — and a `-gc-grace` below GC-1's floor is reported on every
tick rather than silently doing nothing:

```
atlas: background gc: repo: graceDuration violates DESIGN.md §19.2 invariant GC-1:
  got 5m0s, need > 1h0m30.5s (T_write_max=1h0m0s + D_max=30s + epsilon=500ms)
```

What this does **not** claim: a 24-hour soak still has not been run. GC-1
puts a hard floor just over an hour on `T_grace`, so a grace window's
worth of garbage is always on disk by design, and at the default chunk
size that is a large number for a rewrite-heavy workload. A real soak
wants a chunk size matched to its file size *and* background GC enabled.

The soak also exposed a real errno bug: a full backend surfaced as **EIO**
rather than **ENOSPC**, which tells an application its data is corrupt when
the disk is merely full. Fixed on both mounts.

## What has not been run

- **`fsx` for 24 hours** — the clean runs here are minutes. The one long run
  attempted ended in ENOSPC after an hour, for the reason above.
- **The full applicable `generic/` set** — a handful of tests and a partial
  quick group is not §27's "generic groups applicable to a network
  filesystem".
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
