package fuseserver

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aburan28/atlasfs/pkg/repo"
)

// The FUSE-path read benchmark. DESIGN.md §21.1 gates release on
// "cache-hit read ≥ 5 GB/s per node on kernel 6.14+" and "cold read
// ≥ 2 GB/s at 64-way concurrency"; this is the only benchmark in the
// repo that measures the path those numbers are about, because it is the
// only one that crosses the kernel boundary a real client crosses.
//
// §21.2 lists the mechanisms that target assumes — FUSE_PASSTHROUGH,
// writeback caching, large read sizes, multi-queue — none of which this
// build implements. The numbers this produces are therefore a baseline
// for the naive path, not a measurement of the designed one, and the
// gap between them is the honest size of the remaining §21.2 work.

// benchMount publishes a single file of the given size and mounts it,
// returning the path to the file inside the mount.
func benchMount(b *testing.B, size int64) string {
	b.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		b.Skip("no /dev/fuse in this environment")
	}

	src := b.TempDir()
	buf := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(buf)
	if err := os.WriteFile(filepath.Join(src, "data.bin"), buf, 0o644); err != nil {
		b.Fatal(err)
	}

	repoDir := b.TempDir()
	r, err := repo.Open(repoDir)
	if err != nil {
		b.Fatal(err)
	}
	if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
		b.Fatal(err)
	}
	r.Close()

	r2, err := repo.Open(repoDir)
	if err != nil {
		b.Fatal(err)
	}

	mountpoint := b.TempDir()
	mounted := make(chan *fuse.Server, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- Mount(context.Background(), r2, mountpoint, func(s *fuse.Server) { mounted <- s })
	}()

	var server *fuse.Server
	select {
	case server = <-mounted:
	case err := <-errCh:
		b.Fatalf("mount failed: %v", err)
	case <-time.After(10 * time.Second):
		b.Fatal("timed out waiting for FUSE mount")
	}
	b.Cleanup(func() {
		_ = server.Unmount()
		<-errCh
		r2.Close()
	})

	return filepath.Join(mountpoint, "data.bin")
}

// readWhole streams the file through the kernel with a fixed buffer,
// rather than os.ReadFile, so the read size per syscall is explicit.
func readWhole(b *testing.B, path string, buf []byte) {
	b.Helper()
	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	for {
		_, err := f.Read(buf)
		if err == io.EOF {
			return
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFUSEReadSequential is the headline number: a full sequential
// read of one file through a real kernel FUSE mount.
func BenchmarkFUSEReadSequential(b *testing.B) {
	for _, size := range []int64{1 << 20, 16 << 20, 64 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			path := benchMount(b, size)
			buf := make([]byte, 1<<20)

			b.SetBytes(size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				readWhole(b, path, buf)
			}
		})
	}
}

// BenchmarkFUSEReadConcurrent is §21.1's 64-way concurrency shape,
// through the kernel. Each goroutine opens its own file descriptor, so
// this exercises the FUSE server's request concurrency rather than one
// serialized stream.
func BenchmarkFUSEReadConcurrent(b *testing.B) {
	const size = 16 << 20
	path := benchMount(b, size)

	b.SetParallelism(64)
	b.SetBytes(size)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 1<<20)
		for pb.Next() {
			readWhole(b, path, buf)
		}
	})
}

// BenchmarkFUSEStat measures metadata-path latency through the kernel —
// the operation the §10 lease cache exists to make cheap, and the one
// that dominates workloads that walk a tree rather than stream it.
func BenchmarkFUSEStat(b *testing.B) {
	path := benchMount(b, 1<<20)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := os.Stat(path); err != nil {
			b.Fatal(err)
		}
	}
}
