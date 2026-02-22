package dag

import (
	"fmt"
	"os"
	"path/filepath"
)

// SafeWrite writes data to path atomically: tempfile -> fsync -> rename.
// The tempfile is created in the same directory as path to ensure the rename
// is atomic (same filesystem).
func SafeWrite(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmp := f.Name()

	// Clean up on any error
	defer func() {
		if err != nil {
			os.Remove(tmp)
		}
	}()

	if _, err = f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp file: %w", err)
	}
	if err = f.Chmod(perm); err != nil {
		f.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename temp to target: %w", err)
	}
	return nil
}

// SafeAppend opens path for append, writes data, fsyncs, and closes.
func SafeAppend(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open for append: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("append write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("append fsync: %w", err)
	}
	return f.Close()
}
