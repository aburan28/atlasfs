package chunk

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestSplitAllRoundTrip(t *testing.T) {
	data := make([]byte, 10*1024+7) // deliberately not a multiple of chunk size
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	chunks := SplitBytes(data, 4096)

	var got []byte
	for _, c := range chunks {
		got = append(got, c.Data...)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("reassembled data mismatch: got %d bytes, want %d", len(got), len(data))
	}

	wantChunks := (len(data) + 4095) / 4096
	if len(chunks) != wantChunks {
		t.Fatalf("got %d chunks, want %d", len(chunks), wantChunks)
	}
	for _, c := range chunks {
		if Sum(c.Data) != c.ID {
			t.Fatalf("chunk ID does not match content hash")
		}
	}
}

func TestSplitEmpty(t *testing.T) {
	chunks := SplitBytes(nil, 4096)
	if len(chunks) != 0 {
		t.Fatalf("expected no chunks for empty input, got %d", len(chunks))
	}
}

func TestSplitDeterministic(t *testing.T) {
	data := bytes.Repeat([]byte("atlasfs"), 10000)
	a := SplitBytes(data, 4096)
	b := SplitBytes(data, 4096)
	if len(a) != len(b) {
		t.Fatalf("chunk count differs between runs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			t.Fatalf("chunk %d ID differs between runs", i)
		}
	}
}

func TestDedupIdenticalContentSameID(t *testing.T) {
	// Two files with an identical 4096-byte region should produce the
	// same chunk ID for that region — the whole-file/fixed-block dedup
	// property DESIGN.md §15 relies on.
	block := bytes.Repeat([]byte{0x42}, 4096)

	c1 := Sum(block)
	c2 := Sum(append([]byte(nil), block...))
	if c1 != c2 {
		t.Fatalf("identical bytes produced different chunk IDs")
	}
}

func TestIDHexRoundTrip(t *testing.T) {
	id := Sum([]byte("hello atlasfs"))
	s := id.String()
	back, err := IDFromHex(s)
	if err != nil {
		t.Fatal(err)
	}
	if back != id {
		t.Fatalf("hex round trip mismatch")
	}
}
