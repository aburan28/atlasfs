// Package chunk implements content-addressed chunking (DESIGN.md §5.1).
//
// CDC (FastCDC) is off by default per DESIGN.md §15 — chunking here is
// fixed-size, which is the default policy for general subtrees. Chunk
// identity names plaintext; encryption/compression are locator-level
// properties (§5.4), not part of the hash.
package chunk

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"

	"lukechampine.com/blake3"
)

// ID is a content hash: BLAKE3-256 of the chunk's plaintext bytes.
type ID [32]byte

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Hex is the two-character prefix used to shard chunk/container storage
// (DESIGN.md §5.4/§5.5's atlas/c/{region}/{shard}/... layout).
func (id ID) Shard() string { return hex.EncodeToString(id[:1]) }

func IDFromHex(s string) (ID, error) {
	var id ID
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(b) != len(id) {
		return id, fmt.Errorf("chunk: bad id length %d", len(b))
	}
	copy(id[:], b)
	return id, nil
}

// Sum returns the content-addressed ID of b.
func Sum(b []byte) ID {
	return ID(blake3.Sum256(b))
}

// DefaultSize is the default fixed chunk size for general subtrees
// (DESIGN.md §14.3). Checkpoint subtrees should use 16 MiB.
const DefaultSize = 4 << 20 // 4 MiB

// Chunk is a single content-addressed piece of a file, materialized in
// memory. Chunkers only hold one of these at a time; callers persist it
// before requesting the next.
type Chunk struct {
	ID   ID
	Data []byte
}

// Chunker splits an io.Reader into fixed-size, content-addressed chunks.
// It is the "whole-file, fixed-block hashing" path from DESIGN.md §15 —
// the default, measured-before-paying-for-CDC policy.
type Chunker struct {
	r    io.Reader
	size int
	buf  []byte
	err  error
}

// NewChunker returns a Chunker reading from r, splitting into chunks of
// at most size bytes. size <= 0 uses DefaultSize.
func NewChunker(r io.Reader, size int) *Chunker {
	if size <= 0 {
		size = DefaultSize
	}
	return &Chunker{r: r, size: size}
}

// Next returns the next chunk, or io.EOF when the input is exhausted.
// The returned Chunk.Data is only valid until the next call to Next.
func (c *Chunker) Next() (Chunk, error) {
	if c.err != nil {
		return Chunk{}, c.err
	}
	if c.buf == nil {
		c.buf = make([]byte, c.size)
	}
	n, err := io.ReadFull(c.r, c.buf)
	switch err {
	case nil:
		// full-size chunk; loop below on subsequent call may still hit EOF
	case io.ErrUnexpectedEOF:
		// short final chunk
		err = nil
	case io.EOF:
		c.err = io.EOF
		return Chunk{}, io.EOF
	default:
		c.err = err
		return Chunk{}, err
	}
	data := append([]byte(nil), c.buf[:n]...)
	return Chunk{ID: Sum(data), Data: data}, nil
}

// SplitAll reads r fully and returns every chunk. Convenience wrapper
// around Chunker for small inputs and tests.
func SplitAll(r io.Reader, size int) ([]Chunk, error) {
	ck := NewChunker(r, size)
	var out []Chunk
	for {
		c, err := ck.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
}

// SplitBytes is SplitAll over an in-memory buffer.
func SplitBytes(b []byte, size int) []Chunk {
	chunks, err := SplitAll(bytes.NewReader(b), size)
	if err != nil {
		// bytes.Reader never errors
		panic(err)
	}
	return chunks
}
