package repo

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/aburan28/atlasfs/pkg/metadb"
)

// TestFileReaderConcurrentReads pins the assumption that was wrong: one
// FileReader backs one open fd, but the kernel serves concurrent READ
// requests on a single fd from several go-fuse goroutines at once. The
// chunk cache was an unsynchronised map, so a plain sequential read of a
// large file through a mount could take concurrent map writes and kill
// the process with a fatal error — not a panic a caller can recover.
//
// This is the regression test the existing suite lacked: nothing read one
// handle from two goroutines, so `-race` had nothing to see. It surfaced
// in CI as a crash inside the 16 MiB read benchmark.
func TestFileReaderConcurrentReads(t *testing.T) {
	r, _ := openRelaxedWithClock(t)
	ctx := context.Background()

	// Big enough for many chunks, so the cache is exercised and evicted.
	data := make([]byte, 512*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	r.ChunkSize = 4096
	writeCommit(t, r, metadb.RootInode, "big.bin", data)

	_, rec, err := r.Resolve("/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}

	const readers = 8
	var wg sync.WaitGroup
	errs := make([]error, readers)
	for i := range readers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Deliberately unaligned and small: a chunk-aligned read
			// large enough to hold a whole chunk takes ReadAt's zero-copy
			// fast path, which never touches the cache — so an aligned
			// test would exercise none of this. Overlapping ranges are
			// also on purpose: the interesting collisions are two
			// goroutines wanting the same chunk at once.
			buf := make([]byte, 1000)
			for off := int64(i * 331); off+int64(len(buf)) <= int64(len(data)); off += 997 {
				n, err := fr.ReadAt(buf, off)
				if err != nil {
					errs[i] = err
					return
				}
				if !bytes.Equal(buf[:n], data[off:off+int64(n)]) {
					errs[i] = errMismatch
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("reader %d: %v", i, err)
		}
	}
}

var errMismatch = errConst("concurrent read returned the wrong bytes")

type errConst string

func (e errConst) Error() string { return string(e) }
