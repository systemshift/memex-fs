package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// SearchRootDir is the /search/ directory. Lookup treats the name as a query.
type SearchRootDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*SearchRootDir)(nil))
var _ = (fs.NodeReaddirer)((*SearchRootDir)(nil))
var _ = (fs.NodeGetattrer)((*SearchRootDir)(nil))

func (d *SearchRootDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("search")
	return fs.OK
}

func (d *SearchRootDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Empty listing — queries are provided via Lookup
	return fs.NewListDirStream(nil), fs.OK
}

func (d *SearchRootDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Any name is treated as a search query
	results := d.repo.Search.Search(name, 100)
	if len(results) == 0 {
		return nil, syscall.ENOENT
	}
	dir := &SearchResultsDir{repo: d.repo, query: name}
	child := d.NewInode(ctx, dir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("search/" + name),
	})
	return child, fs.OK
}

// SearchResultsDir is /search/{query}/ — lists matching nodes as symlinks.
type SearchResultsDir struct {
	fs.Inode
	repo  *dag.Repository
	query string
}

var _ = (fs.NodeLookuper)((*SearchResultsDir)(nil))
var _ = (fs.NodeReaddirer)((*SearchResultsDir)(nil))
var _ = (fs.NodeGetattrer)((*SearchResultsDir)(nil))

func (d *SearchResultsDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("search/" + d.query)
	return fs.OK
}

func (d *SearchResultsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	results := d.repo.Search.Search(d.query, 100)
	entries := make([]fuse.DirEntry, len(results))
	for i, id := range results {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("search/" + d.query + "/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *SearchResultsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Verify the node exists and matches the query
	results := d.repo.Search.Search(d.query, 100)
	found := false
	for _, id := range results {
		if id == name {
			found = true
			break
		}
	}
	if !found {
		return nil, syscall.ENOENT
	}

	sym := &SearchSymlink{nodeID: name}
	child := d.NewInode(ctx, sym, fs.StableAttr{
		Mode: syscall.S_IFLNK,
		Ino:  stableIno("search/" + d.query + "/" + name),
	})
	return child, fs.OK
}

// SearchSymlink points to ../../nodes/{id}.
type SearchSymlink struct {
	fs.Inode
	nodeID string
}

var _ = (fs.NodeReadlinker)((*SearchSymlink)(nil))
var _ = (fs.NodeGetattrer)((*SearchSymlink)(nil))

func (s *SearchSymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte("../../nodes/" + s.nodeID), fs.OK
}

func (s *SearchSymlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	target := "../../nodes/" + s.nodeID
	out.Mode = 0777 | syscall.S_IFLNK
	out.Size = uint64(len(target))
	return fs.OK
}
