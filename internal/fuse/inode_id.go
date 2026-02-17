package fuse

import "hash/fnv"

// stableIno returns a stable inode number for a given path string.
func stableIno(path string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(path))
	return h.Sum64()
}
