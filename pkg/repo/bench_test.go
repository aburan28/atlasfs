package repo

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/aburan28/atlasfs/pkg/chunk"
)

// Benchmarks for the layers DESIGN.md §21.1 states throughput targets
// for. They exist so those targets stop being assertions: §21.1 claims
// "cache-hit read ≥ 5 GB/s per node" and "cold read ≥ 2 GB/s at 64-way
// concurrency", and until this file existed nothing in the repo had ever
// measured either.
//
// What these do and do not measure, stated plainly so the numbers are not
// over-read:
//
//   - The backend here is pkg/store/local (a directory on whatever disk
//     the run happens to use), not S3. A "cold" read below is therefore a
//     local-disk read, which is an optimistic stand-in for a real cold
//     read over a network object store — it isolates AtlasFS's own
//     per-chunk overhead (locator lookup, fetch, hash verification,
//     reassembly) from network latency, which is the useful thing to
//     track in CI, but it is not the §21.1 cold-read number.
//   - These are library-level reads. The FUSE-path number, which is the
//     one §21.1 actually gates on, is measured in
//     pkg/fuseserver/bench_test.go — it includes the kernel round trip
//     these do not.

func benchRepo(b *testing.B, fileSize int64) (*Repo, string) {
	b.Helper()
	ctx := context.Background()
	src := b.TempDir()

	buf := make([]byte, fileSize)
	// Deterministic pseudo-random content: incompressible enough to be
	// realistic, but seeded so a re-run benchmarks the same bytes.
	rng := rand.New(rand.NewSource(1))
	rng.Read(buf)
	if err := os.WriteFile(filepath.Join(src, "data.bin"), buf, 0o644); err != nil {
		b.Fatal(err)
	}

	r, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		b.Fatal(err)
	}
	return r, "/data.bin"
}

// BenchmarkReadColdSequential opens a fresh FileReader per iteration, so
// every chunk misses FileReader's cache and is refetched from the backend
// and re-verified. This is the per-chunk overhead floor.
func BenchmarkReadColdSequential(b *testing.B) {
	for _, size := range []int64{1 << 20, 16 << 20, 64 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			ctx := context.Background()
			r, path := benchRepo(b, size)
			defer r.Close()
			_, rec, err := r.Resolve(path)
			if err != nil {
				b.Fatal(err)
			}

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fr, err := r.OpenFile(ctx, rec)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := fr.ReadAll(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadWarmRandom reuses one FileReader over a file small enough
// to sit entirely in its fileReaderCacheEntries-sized cache, so after the
// first pass every read is a cache hit. This is the closest library-level
// analogue to §21.1's "cache-hit read" target.
func BenchmarkReadWarmRandom(b *testing.B) {
	ctx := context.Background()
	// fileReaderCacheEntries chunks exactly, so nothing is ever evicted.
	size := int64(fileReaderCacheEntries) * int64(chunk.DefaultSize)
	r, path := benchRepo(b, size)
	defer r.Close()
	_, rec, err := r.Resolve(path)
	if err != nil {
		b.Fatal(err)
	}
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := fr.ReadAll(); err != nil { // warm every chunk
		b.Fatal(err)
	}

	const readSize = 128 << 10
	buf := make([]byte, readSize)
	rng := rand.New(rand.NewSource(2))

	b.SetBytes(readSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := rng.Int63n(size - readSize)
		if _, err := fr.ReadAt(buf, off); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadColdConcurrent is §21.1's "cold read at 64-way
// concurrency" shape: independent readers, each with its own FileReader,
// against one published file.
func BenchmarkReadColdConcurrent(b *testing.B) {
	ctx := context.Background()
	const size = 16 << 20
	r, path := benchRepo(b, size)
	defer r.Close()
	_, rec, err := r.Resolve(path)
	if err != nil {
		b.Fatal(err)
	}

	b.SetParallelism(64)
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	// Errors are collected rather than fataled in place: b.Fatal calls
	// runtime.Goexit, and a RunParallel worker that exits that way never
	// signals completion, so a failure would hang the benchmark instead
	// of failing it.
	var failed atomic.Pointer[error]
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fr, err := r.OpenFile(ctx, rec)
			if err != nil {
				failed.CompareAndSwap(nil, &err)
				return
			}
			if _, err := fr.ReadAll(); err != nil {
				failed.CompareAndSwap(nil, &err)
				return
			}
		}
	})
	if err := failed.Load(); err != nil {
		b.Fatal(*err)
	}
}

// BenchmarkPublish measures the ingest side: chunking, BLAKE3 hashing,
// packing, and the metadata writes for one file.
func BenchmarkPublish(b *testing.B) {
	ctx := context.Background()
	const size = 16 << 20
	buf := make([]byte, size)
	rand.New(rand.NewSource(3)).Read(buf)

	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := b.TempDir()
		if err := os.WriteFile(filepath.Join(src, "data.bin"), buf, 0o644); err != nil {
			b.Fatal(err)
		}
		r, err := Open(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		r.Close()
		b.StartTimer()
	}
}
