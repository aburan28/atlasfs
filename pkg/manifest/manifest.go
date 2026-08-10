// Package manifest implements the manifest format from DESIGN.md §5.2/§5.3.
//
// A manifest is the ordered chunk list for one file at one version. It is
// itself a content-addressed, immutable blob — never a metadata-store
// value — which is what keeps it out of FoundationDB's (or here, bbolt's)
// value/transaction size limits for large files. manifest_id is the
// BLAKE3-256 hash of the manifest's canonical encoding, so two files with
// identical content produce byte-identical manifests.
package manifest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"lukechampine.com/blake3"
)

const FormatVersion uint16 = 1

// Entry flags (reserved for future use — e.g. marking a chunk as
// already-compressed at rest). None are defined yet.
const (
	FlagNone uint32 = 0
)

type Entry struct {
	ChunkID chunk.ID
	Length  uint32
	Flags   uint32
}

// Manifest is the ordered chunk list for a file's content at one version.
type Manifest struct {
	FormatVersion uint16
	ContentSize   uint64
	ChunkSizeHint uint32
	Entries       []Entry
}

// ID is the manifest's content address: BLAKE3-256 of its canonical
// encoding (DESIGN.md §5.2).
type ID [32]byte

func (id ID) String() string { return fmt.Sprintf("%x", id[:]) }

// New builds a manifest from an ordered list of chunks as produced by
// pkg/chunk. chunkSizeHint is the nominal chunk size used to produce
// them (informational; the last entry is typically shorter).
func New(chunks []chunk.Chunk, chunkSizeHint int) *Manifest {
	m := &Manifest{
		FormatVersion: FormatVersion,
		ChunkSizeHint: uint32(chunkSizeHint),
		Entries:       make([]Entry, len(chunks)),
	}
	var total uint64
	for i, c := range chunks {
		m.Entries[i] = Entry{ChunkID: c.ID, Length: uint32(len(c.Data)), Flags: FlagNone}
		total += uint64(len(c.Data))
	}
	m.ContentSize = total
	return m
}

// Encode writes the manifest's canonical binary encoding. The encoding is
// deliberately simple and fixed-width per field so that identical
// manifests always produce identical bytes, which is the only property
// ID() depends on.
func (m *Manifest) Encode(w io.Writer) error {
	bw := &bufErrWriter{w: w}
	bw.u16(m.FormatVersion)
	bw.u64(m.ContentSize)
	bw.u32(m.ChunkSizeHint)
	bw.u32(uint32(len(m.Entries)))
	for _, e := range m.Entries {
		bw.bytes(e.ChunkID[:])
		bw.u32(e.Length)
		bw.u32(e.Flags)
	}
	return bw.err
}

// EncodeBytes returns the canonical encoding as a byte slice.
func (m *Manifest) EncodeBytes() []byte {
	var buf bytes.Buffer
	// Encode over an in-memory buffer never fails.
	_ = m.Encode(&buf)
	return buf.Bytes()
}

// ID computes the manifest's content address over its canonical encoding.
func (m *Manifest) ID() ID {
	return ID(blake3.Sum256(m.EncodeBytes()))
}

// Decode parses a manifest previously produced by Encode.
func Decode(r io.Reader) (*Manifest, error) {
	br := &bufErrReader{r: r}
	m := &Manifest{}
	m.FormatVersion = br.u16()
	if br.err == nil && m.FormatVersion != FormatVersion {
		return nil, fmt.Errorf("manifest: unsupported format version %d", m.FormatVersion)
	}
	m.ContentSize = br.u64()
	m.ChunkSizeHint = br.u32()
	n := br.u32()
	if br.err != nil {
		return nil, br.err
	}
	m.Entries = make([]Entry, n)
	for i := range m.Entries {
		var id chunk.ID
		br.readBytes(id[:])
		length := br.u32()
		flags := br.u32()
		if br.err != nil {
			return nil, br.err
		}
		m.Entries[i] = Entry{ChunkID: id, Length: length, Flags: flags}
	}
	return m, nil
}

func DecodeBytes(b []byte) (*Manifest, error) { return Decode(bytes.NewReader(b)) }

// --- tiny deterministic binary helpers -------------------------------------

type bufErrWriter struct {
	w   io.Writer
	err error
}

func (b *bufErrWriter) u16(v uint16) {
	if b.err != nil {
		return
	}
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	_, b.err = b.w.Write(buf[:])
}

func (b *bufErrWriter) u32(v uint32) {
	if b.err != nil {
		return
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	_, b.err = b.w.Write(buf[:])
}

func (b *bufErrWriter) u64(v uint64) {
	if b.err != nil {
		return
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	_, b.err = b.w.Write(buf[:])
}

func (b *bufErrWriter) bytes(v []byte) {
	if b.err != nil {
		return
	}
	_, b.err = b.w.Write(v)
}

type bufErrReader struct {
	r   io.Reader
	err error
}

func (b *bufErrReader) u16() uint16 {
	var buf [2]byte
	b.readBytes(buf[:])
	return binary.BigEndian.Uint16(buf[:])
}

func (b *bufErrReader) u32() uint32 {
	var buf [4]byte
	b.readBytes(buf[:])
	return binary.BigEndian.Uint32(buf[:])
}

func (b *bufErrReader) u64() uint64 {
	var buf [8]byte
	b.readBytes(buf[:])
	return binary.BigEndian.Uint64(buf[:])
}

func (b *bufErrReader) readBytes(dst []byte) {
	if b.err != nil {
		return
	}
	_, b.err = io.ReadFull(b.r, dst)
}
