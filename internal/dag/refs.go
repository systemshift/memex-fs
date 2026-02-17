package dag

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	gocid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
)

// RefStore manages human-readable ID -> CID mappings as files.
// Each ref is a file in the refs/ directory whose content is the CID string.
// Filenames use URL-safe encoding: colons become double underscores.
type RefStore struct {
	dir string
}

// NewRefStore creates a RefStore at the given directory.
func NewRefStore(dir string) (*RefStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create refs dir: %w", err)
	}
	return &RefStore{dir: dir}, nil
}

func refFilename(id string) string {
	return strings.ReplaceAll(id, ":", "__")
}

func refIDFromFilename(name string) string {
	return strings.ReplaceAll(name, "__", ":")
}

// Set writes a ref mapping id -> cid.
func (r *RefStore) Set(id string, c gocid.Cid) error {
	path := filepath.Join(r.dir, refFilename(id))
	encoded, _ := multibase.Encode(multibase.Base32, c.Bytes())
	return os.WriteFile(path, []byte(encoded), 0644)
}

// Get resolves a human-readable ID to a CID.
func (r *RefStore) Get(id string) (gocid.Cid, error) {
	path := filepath.Join(r.dir, refFilename(id))
	data, err := os.ReadFile(path)
	if err != nil {
		return gocid.Undef, fmt.Errorf("ref not found: %s", id)
	}
	_, cidBytes, err := multibase.Decode(strings.TrimSpace(string(data)))
	if err != nil {
		return gocid.Undef, fmt.Errorf("decode ref CID: %w", err)
	}
	return gocid.Cast(cidBytes)
}

// Delete removes a ref.
func (r *RefStore) Delete(id string) error {
	path := filepath.Join(r.dir, refFilename(id))
	return os.Remove(path)
}

// Has checks if a ref exists.
func (r *RefStore) Has(id string) bool {
	path := filepath.Join(r.dir, refFilename(id))
	_, err := os.Stat(path)
	return err == nil
}

// List returns all ref IDs.
func (r *RefStore) List() ([]string, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, fmt.Errorf("list refs: %w", err)
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ids = append(ids, refIDFromFilename(e.Name()))
	}
	return ids, nil
}
