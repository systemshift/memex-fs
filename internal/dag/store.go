package dag

import (
	"fmt"
	"os"
	"path/filepath"

	gocid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
)

// CidUndef is the undefined/zero CID value, exported for use by other packages.
var CidUndef = gocid.Undef

// ObjectStore manages CID-addressed immutable objects on disk.
type ObjectStore struct {
	dir string // path to objects/ directory
}

// NewObjectStore creates an ObjectStore at the given directory.
func NewObjectStore(dir string) (*ObjectStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create objects dir: %w", err)
	}
	return &ObjectStore{dir: dir}, nil
}

// ComputeCID computes a CIDv1 (raw codec, SHA2-256) for the given data.
func ComputeCID(data []byte) (gocid.Cid, error) {
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		return gocid.Undef, fmt.Errorf("multihash: %w", err)
	}
	return gocid.NewCidV1(gocid.Raw, mh), nil
}

// CIDToFilename returns the base32lower encoding of a CID for use as a filename.
func CIDToFilename(c gocid.Cid) string {
	encoded, _ := multibase.Encode(multibase.Base32, c.Bytes())
	return encoded
}

// Put writes data to the object store, returning the CID.
// If the object already exists, this is a no-op.
func (s *ObjectStore) Put(data []byte) (gocid.Cid, error) {
	c, err := ComputeCID(data)
	if err != nil {
		return gocid.Undef, err
	}
	path := filepath.Join(s.dir, CIDToFilename(c))
	if _, err := os.Stat(path); err == nil {
		return c, nil // already exists
	}
	if err := SafeWrite(path, data, 0644); err != nil {
		return gocid.Undef, fmt.Errorf("write object: %w", err)
	}
	return c, nil
}

// Get reads an object by CID.
func (s *ObjectStore) Get(c gocid.Cid) ([]byte, error) {
	path := filepath.Join(s.dir, CIDToFilename(c))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", c, err)
	}
	return data, nil
}

// Has checks if an object exists.
func (s *ObjectStore) Has(c gocid.Cid) bool {
	path := filepath.Join(s.dir, CIDToFilename(c))
	_, err := os.Stat(path)
	return err == nil
}
