package repoopen

import (
	"context"
	"strings"
	"testing"
)

// TestPosixClassIsRefusedByLocalOpen: accepting -class=posix here would
// hand back a mount carrying the class's name and cost with none of its
// guarantee, since §10.6's D=0 comes from recalling remote holders and
// an in-process coherence manager has none. An error naming the right
// tool beats a filesystem that quietly lies about its consistency.
func TestPosixClassIsRefusedByLocalOpen(t *testing.T) {
	_, err := Open(context.Background(), t.TempDir(), Params{Class: "posix"})
	if err == nil {
		t.Fatal("expected posix to be refused by a local open")
	}
	if !strings.Contains(err.Error(), "atlas-mds") {
		t.Fatalf("error should point at the tool that does serve posix, got: %v", err)
	}
}
