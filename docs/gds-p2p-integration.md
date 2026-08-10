# GPUDirect Storage and Dragonfly P2P: integration plan

Scope for two integrations that pull in opposite directions and meet in
the middle. GPUDirect Storage (NVIDIA cuFile) is about the **last hop** —
getting bytes from a local NVMe into GPU memory without touching the CPU.
Dragonfly (d7y.io) is about the **first hop** — getting bytes onto that
node from a peer instead of from the origin bucket. AtlasFS sits between
them, and both plug into the same layer: `store.Backend`.

Sources: NVIDIA's [GPUDirect Storage API reference](https://docs.nvidia.com/gpudirect-storage/api-reference-guide/index.html)
and [best practices guide](https://docs.nvidia.com/gpudirect-storage/best-practices-guide/index.html);
Dragonfly's [dfdaemon configuration reference](https://d7y.io/docs/v2.2.0/reference/configuration/client/dfdaemon/).

## Status at a glance

| Layer | State | Verified? |
|---|---|---|
| Caller-supplied destination (`store.GetInto`, `pack.FetchInto`) | Built | Yes — tests + benchmarks |
| 4 KiB container alignment (`pack.GDSAlignment`) | Built, opt-in | Yes — alignment + padding-cost tests |
| Host/device destination (`store.Dest`) | Built | Host path tested; device path has no consumer |
| cuFile cgo binding (`pkg/gds`, `-tags cufile`) | Written | **No — never compiled.** No CUDA here |
| GDS-aware backend (open O_DIRECT, `cuFileRead` into `Dest`) | Not built | — |
| Dragonfly transport (`pkg/store/dragonfly`) | Built, wired to S3 + CLI + CSI | Yes — against a real forwarding proxy |
| Dragonfly seed-peer / manager integration | Not built, and not planned | — |

## Part 1: GPUDirect Storage

### Why the naive version does not work

The tempting shape is "add a cuFile backend and call `cuFileRead`". Two
things in AtlasFS's existing design block that, and both were found by
reading the API docs rather than by trying it:

**Containers are packed unaligned.** DESIGN.md §5.5 packs chunks back to
back, so a chunk's offset inside its container is wherever the previous
chunk ended. NVIDIA's best-practices guide defines an I/O as unaligned if
*any* of file offset, size, device pointer, or device-buffer offset is
not 4 KiB aligned, and serves unaligned I/O through internal GPU bounce
buffers. So a GDS read of a normally-packed AtlasFS chunk is unaligned by
construction, every single time, and the feature degrades into a slower
POSIX read with extra bookkeeping. **Alignment is a precondition, not a
tuning knob.**

This is now implemented as opt-in (`pack.GDSAlignment`, `-gds-align`)
because it is not free. Measured on 200 × 1 KiB files: containers grow
**3.98×**. DESIGN.md §14 exists to pack small files tightly, and
alignment directly undoes that. It is right for a dataset of large
tensors read by GPUs and wrong for a tree of small files read by CPUs,
which is why it is a per-deployment flag rather than a default.

**A `[]byte` cannot name GPU memory.** `cuFileRead(fh, bufPtr_base, size,
file_offset, bufPtr_offset)` takes the *registered base* of a device
allocation plus an offset into it. `store.Dest` mirrors that shape
exactly — base and offset kept separate — rather than collapsing them,
because collapsing them here would force every caller to reconstruct them
there.

### Layers, bottom to top

1. **`pkg/gds`** — cgo binding: `cuFileDriverOpen/Close`,
   `cuFileHandleRegister/Deregister`, `cuFileBufRegister/Deregister`,
   `cuFileRead`. Behind `-tags cufile`; a stub otherwise that fails every
   call with `ErrUnavailable`. The stub **fails closed rather than
   falling back to a host read**: a silent host fallback would hand back
   exactly the CPU bounce buffer the caller was trying to eliminate,
   while reporting success.

2. **`store.Dest` + `store.GetterIntoDest`** — a destination that is
   either host or device memory, and the optional backend capability that
   fills one. Deliberately separate from `GetterInto` so the four
   existing backends do not grow a parameter none of them can honor.

3. **A GDS-aware local backend** (not built) — opens container objects
   with `O_DIRECT`, registers each fd with `cuFileHandleRegister`, and
   implements `GetIntoDest` by calling `cuFileRead`. This is the piece
   that needs a GPU to write honestly, because every design question in
   it (handle cache lifetime, fd-per-container vs shared, registration
   churn) is answered by measurement.

4. **Repo plumbing** (not built) — `FileReader` would need a
   `ReadAtDest` alongside `ReadAt` so a caller can name a device buffer.
   Straightforward once (3) exists; premature before.

### The verification problem, stated plainly

DESIGN.md §24.4 makes reads self-verifying: `pack.FetchInto` hashes the
destination after the read, so a backend cannot serve wrong bytes
undetected. That works **only because host memory can be read back**.

DMA into device memory and then hashing on the CPU means copying it back
— forfeiting the entire benefit. A GDS path therefore has exactly two
honest options:

- **Verify on the GPU.** A BLAKE3 kernel over the landed buffer. Real
  work, and the only option that preserves §24.4 as written.
- **Declare verification waived for the GDS path**, and say so in the
  consistency documentation, accepting that a corrupted container becomes
  a silent wrong answer for GPU readers.

There is no third option, and the seam is built so that the choice has to
be made explicitly rather than fallen into. Whoever builds (3) picks one.

### What is needed to finish

A machine with an NVIDIA GPU, CUDA toolkit, `libcufile`, and a
GDS-supported filesystem. `pkg/gds`'s cgo file has never been through a
compiler; expect to fix errors there before expecting it to run.

## Part 2: Dragonfly P2P

### Why this is a cost feature, not a speed feature

DESIGN.md §12.2 is explicit that request charges, not bandwidth, are the
binding constraint: its worked example puts a cold fleet at 1.19M GET/s
and **$1,717/hour in S3 request charges alone**. Every one of those GETs
is for an immutable, content-addressed container, and across N nodes
training on one dataset, N−1 of them are asking the origin for bytes a
neighbour already has.

Dragonfly collapses that: the first peer fetches back-to-source, the rest
get it from the mesh. Origin GETs go from once-per-node to
once-per-cluster, and cross-AZ egress goes with them. Correctness is
unaffected for a reason specific to this design — a container is
immutable and named by content, so a peer cache needs no invalidation
protocol at all. This is the cheapest large win available to AtlasFS, and
it required no new protocol code.

### How it is wired

dfdaemon exposes an HTTP proxy (default `:4001`). Rather than teach
AtlasFS a peer protocol, `pkg/store/dragonfly` hands back an
`*http.Client` that proxies through dfdaemon and stamps two headers:

- `X-Dragonfly-Use-P2P: true` — opts in regardless of cluster-side regex
  rules, so AtlasFS does not depend on proxy configuration matching its
  bucket layout.
- `X-Dragonfly-Prefetch: true` — on a range request, fetch the whole
  task. This is the right default **because of how AtlasFS reads**: a
  container is read by many small ranges, one per chunk, so prefetching
  once turns dozens of independent P2P tasks into one and every
  subsequent chunk hits the local peer cache. Overridable for deployments
  reading a few scattered bytes out of very large containers.

That client is handed to the S3 backend via `aws.Config.HTTPClient`.
**`pkg/store/s3` is untouched and unaware.**

Enable with `-dragonfly-proxy=auto` on the CLI, or `dragonflyProxy` in a
CSI VolumeContext.

### Deliberately out of scope

No Manager or Scheduler integration, no Seed Peer registration, no
`dfget`. Those are cluster-side concerns. From AtlasFS's side a Dragonfly
deployment is "an HTTP proxy that makes GETs cheaper" — which is the
entire point of integrating a mature P2P system rather than writing one.

### How it was verified

Not with mocks. A real forwarding HTTP proxy stands in for dfdaemon,
and the tests check the **path**, not just the payload — because if the
wiring were wrong the bytes would still arrive, straight from the origin.
The load-bearing assertion is that requests arrive as absolute URIs,
which is what distinguishes a proxied request from a direct one. Covered:
header stamping, `Range` survival (a proxy that dropped `Range` would
return whole containers where chunks were asked for — correct-looking
bytes at catastrophically wrong offsets), prefetch opt-out,
non-mutation of the caller's request (the AWS SDK reuses and signs
requests across retries), and a full S3 publish/read cycle where 5
requests traversed the proxy with the capability probe intact.

## Combined picture

The two integrations compose without touching each other:

```
GPU memory  <--cuFileRead--  local NVMe container  <--dfdaemon P2P--  peer
                                                    <--back-to-source-- S3
```

Dragonfly decides *how a container arrives on the node*; GDS decides *how
it gets from the node into the GPU*. They meet at the container object,
which is why `pack.GDSAlignment` matters to both: an aligned container is
GDS-readable, and it is still just an object as far as Dragonfly is
concerned.

The sequencing that follows from this: **do Dragonfly first**. It is
done, needs no special hardware, and attacks the constraint DESIGN.md
itself calls binding. GDS needs a GPU to finish and buys throughput on a
path that is currently ~5× off its target for reasons that have nothing
to do with GPUs (see the README's §21.2 note).
