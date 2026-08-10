package gds

import (
	"context"
	"errors"
	"testing"
)

// TestAlignedMatchesTheDocumentedRule encodes NVIDIA's definition of an
// unaligned GDS I/O: file offset, size, device pointer, and device
// buffer offset must each be 4 KiB aligned, and missing any one of them
// sends the read through GPU bounce buffers. Getting this predicate
// wrong would mean a deployment believing it had aligned containers
// while every read paid the copy anyway.
func TestAlignedMatchesTheDocumentedRule(t *testing.T) {
	const k = 4096
	for _, tc := range []struct {
		name          string
		fileOff, size int64
		devPtr        uintptr
		devOff        int64
		want          bool
	}{
		{"all aligned", k, 2 * k, k, 0, true},
		{"zero everything but ptr", 0, k, k, 0, true},
		{"file offset off by one", k + 1, k, k, 0, false},
		{"size not a multiple", k, k + 1, k, 0, false},
		{"device pointer unaligned", k, k, k + 8, 0, false},
		{"device offset unaligned", k, k, k, 1, false},
		// The case that motivated pack.GDSAlignment: chunks packed back
		// to back land on arbitrary offsets.
		{"tightly packed chunk offset", 3, k, k, 0, false},
	} {
		if got := Aligned(tc.fileOff, tc.size, tc.devPtr, tc.devOff); got != tc.want {
			t.Errorf("%s: Aligned(%d,%d,%d,%d) = %v, want %v",
				tc.name, tc.fileOff, tc.size, tc.devPtr, tc.devOff, got, tc.want)
		}
	}
}

// TestStubFailsClosed is the property that matters for a build without
// cuFile: asking for GPUDirect must fail, not silently do a host read.
// A quiet fallback would hand back the CPU bounce buffer the caller was
// trying to eliminate while reporting success — the failure mode most
// likely to go unnoticed in production.
func TestStubFailsClosed(t *testing.T) {
	if Available() {
		t.Skip("built with -tags cufile; this test covers the stub")
	}
	if _, err := OpenDriver(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("OpenDriver = %v, want ErrUnavailable", err)
	}
	if err := RegisterBuffer(0x1000, 4096); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("RegisterBuffer = %v, want ErrUnavailable", err)
	}
	if err := DeregisterBuffer(0x1000); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("DeregisterBuffer = %v, want ErrUnavailable", err)
	}
	var d Driver
	if _, err := d.RegisterFile(3); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("RegisterFile = %v, want ErrUnavailable", err)
	}
	var h FileHandle
	if _, err := h.ReadInto(context.Background(), 0, 0x1000, 0, 4096); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ReadInto = %v, want ErrUnavailable", err)
	}
}
