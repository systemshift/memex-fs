package dag

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSafeWrite_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	data := []byte("hello world")

	if err := SafeWrite(path, data, 0644); err != nil {
		t.Fatalf("SafeWrite: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}

	// Verify permissions
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("perm = %o, want 0644", info.Mode().Perm())
	}
}

func TestSafeWrite_OverwriteExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	if err := SafeWrite(path, []byte("first"), 0644); err != nil {
		t.Fatalf("SafeWrite first: %v", err)
	}
	if err := SafeWrite(path, []byte("second"), 0644); err != nil {
		t.Fatalf("SafeWrite second: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("got %q, want %q", got, "second")
	}
}

func TestSafeWrite_NoPartialFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	// Write initial content
	if err := SafeWrite(path, []byte("original"), 0644); err != nil {
		t.Fatalf("SafeWrite: %v", err)
	}

	// Try to write to a read-only directory (should fail)
	badPath := filepath.Join(dir, "nodir", "test.txt")
	err := SafeWrite(badPath, []byte("bad"), 0644)
	if err == nil {
		t.Fatal("expected error writing to nonexistent dir")
	}

	// Verify no temp files left behind in dir
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "test.txt" {
			t.Fatalf("unexpected file left behind: %s", e.Name())
		}
	}

	// Original is untouched
	got, _ := os.ReadFile(path)
	if string(got) != "original" {
		t.Fatalf("original corrupted: got %q", got)
	}
}

func TestSafeWrite_SameDirectory(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	os.MkdirAll(sub, 0755)

	path := filepath.Join(sub, "data.bin")
	if err := SafeWrite(path, []byte{0x01, 0x02}, 0600); err != nil {
		t.Fatalf("SafeWrite: %v", err)
	}

	// Temp files should be in sub/, not dir/
	entries, _ := os.ReadDir(sub)
	if len(entries) != 1 || entries[0].Name() != "data.bin" {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("unexpected files in sub dir: %v", names)
	}
}

func TestSafeAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.jsonl")

	if err := SafeAppend(path, []byte("line1\n")); err != nil {
		t.Fatalf("SafeAppend 1: %v", err)
	}
	if err := SafeAppend(path, []byte("line2\n")); err != nil {
		t.Fatalf("SafeAppend 2: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "line1\nline2\n" {
		t.Fatalf("got %q, want %q", got, "line1\nline2\n")
	}
}

func TestSafeAppend_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "new.log")

	if err := SafeAppend(path, []byte("first\n")); err != nil {
		t.Fatalf("SafeAppend: %v", err)
	}

	got, _ := os.ReadFile(path)
	if string(got) != "first\n" {
		t.Fatalf("got %q, want %q", got, "first\n")
	}
}
