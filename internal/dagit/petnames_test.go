package dagit

import (
	"testing"
)

func TestPetnameFromDID_KnownVectors(t *testing.T) {
	tests := []struct {
		did  string
		want string
	}{
		{
			did:  "did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd",
			want: "rare-frost",
		},
		{
			did:  "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
			want: "clear-dune",
		},
	}
	for _, tt := range tests {
		got := PetnameFromDID(tt.did)
		if got != tt.want {
			t.Errorf("PetnameFromDID(%s) = %q, want %q", tt.did, got, tt.want)
		}
	}
}

func TestPetnameFromDID_Deterministic(t *testing.T) {
	did := "did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd"
	first := PetnameFromDID(did)
	for i := 0; i < 100; i++ {
		got := PetnameFromDID(did)
		if got != first {
			t.Fatalf("non-deterministic on iteration %d: %q vs %q", i, got, first)
		}
	}
}

func TestPetnameFromDID_Format(t *testing.T) {
	did := "did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd"
	name := PetnameFromDID(did)
	// Should be "adjective-noun" format
	parts := splitOnce(name, '-')
	if len(parts) != 2 {
		t.Fatalf("expected adjective-noun, got %q", name)
	}
	if parts[0] == "" || parts[1] == "" {
		t.Fatalf("empty part in %q", name)
	}
}

func splitOnce(s string, sep byte) []string {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return []string{s[:i], s[i+1:]}
		}
	}
	return []string{s}
}
