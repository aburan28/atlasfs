package manifest

import (
	"bytes"
	"testing"

	"github.com/aburan28/atlasfs/pkg/chunk"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("atlasfs-manifest-test-"), 5000)
	chunks := chunk.SplitBytes(data, 4096)
	m := New(chunks, 4096)

	encoded := m.EncodeBytes()
	decoded, err := DecodeBytes(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ContentSize != m.ContentSize {
		t.Fatalf("content size mismatch: got %d want %d", decoded.ContentSize, m.ContentSize)
	}
	if len(decoded.Entries) != len(m.Entries) {
		t.Fatalf("entry count mismatch: got %d want %d", len(decoded.Entries), len(m.Entries))
	}
	for i := range m.Entries {
		if decoded.Entries[i] != m.Entries[i] {
			t.Fatalf("entry %d mismatch: got %+v want %+v", i, decoded.Entries[i], m.Entries[i])
		}
	}
}

func TestIDIsDeterministic(t *testing.T) {
	data := bytes.Repeat([]byte("deterministic"), 1000)
	c1 := chunk.SplitBytes(data, 4096)
	c2 := chunk.SplitBytes(data, 4096)

	m1 := New(c1, 4096)
	m2 := New(c2, 4096)
	if m1.ID() != m2.ID() {
		t.Fatalf("identical content produced different manifest IDs")
	}
}

func TestIDChangesWithContent(t *testing.T) {
	m1 := New(chunk.SplitBytes([]byte("aaaa"), 4096), 4096)
	m2 := New(chunk.SplitBytes([]byte("bbbb"), 4096), 4096)
	if m1.ID() == m2.ID() {
		t.Fatalf("different content produced the same manifest ID")
	}
}

func TestDecodeRejectsBadVersion(t *testing.T) {
	m := New(chunk.SplitBytes([]byte("x"), 4096), 4096)
	encoded := m.EncodeBytes()
	// Corrupt the format_version field (first 2 bytes, big-endian).
	encoded[0] = 0xFF
	encoded[1] = 0xFF
	if _, err := DecodeBytes(encoded); err == nil {
		t.Fatal("expected error decoding an unsupported format version")
	}
}
