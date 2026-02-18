package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// RelatedRootDir is the /related/ directory. Lookup by node ID.
type RelatedRootDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*RelatedRootDir)(nil))
var _ = (fs.NodeReaddirer)((*RelatedRootDir)(nil))
var _ = (fs.NodeGetattrer)((*RelatedRootDir)(nil))

func (d *RelatedRootDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("related")
	return fs.OK
}

func (d *RelatedRootDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// List all node IDs so users can tab-complete
	ids, err := d.repo.ListNodes(0)
	if err != nil {
		return fs.NewListDirStream(nil), fs.OK
	}
	entries := make([]fuse.DirEntry, len(ids))
	for i, id := range ids {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("related/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *RelatedRootDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Verify node exists
	if _, err := d.repo.GetNode(name); err != nil {
		return nil, syscall.ENOENT
	}
	dir := &RelatedResultsDir{repo: d.repo, nodeID: name}
	child := d.NewInode(ctx, dir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("related/" + name),
	})
	return child, fs.OK
}

// RelatedResultsDir is /related/{id}/ — lists related nodes as symlinks.
type RelatedResultsDir struct {
	fs.Inode
	repo   *dag.Repository
	nodeID string
}

var _ = (fs.NodeLookuper)((*RelatedResultsDir)(nil))
var _ = (fs.NodeReaddirer)((*RelatedResultsDir)(nil))
var _ = (fs.NodeGetattrer)((*RelatedResultsDir)(nil))

func (d *RelatedResultsDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("related/" + d.nodeID)
	return fs.OK
}

func (d *RelatedResultsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	related := d.repo.Relatedness.Related(d.nodeID, 50)
	entries := make([]fuse.DirEntry, len(related))
	for i, id := range related {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("related/" + d.nodeID + "/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *RelatedResultsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Verify this node is in the related results
	related := d.repo.Relatedness.Related(d.nodeID, 50)
	found := false
	for _, id := range related {
		if id == name {
			found = true
			break
		}
	}
	if !found {
		return nil, syscall.ENOENT
	}

	sym := &RelatedSymlink{nodeID: name}
	child := d.NewInode(ctx, sym, fs.StableAttr{
		Mode: syscall.S_IFLNK,
		Ino:  stableIno("related/" + d.nodeID + "/" + name),
	})
	return child, fs.OK
}

// RelatedSymlink points to ../../nodes/{id}.
type RelatedSymlink struct {
	fs.Inode
	nodeID string
}

var _ = (fs.NodeReadlinker)((*RelatedSymlink)(nil))
var _ = (fs.NodeGetattrer)((*RelatedSymlink)(nil))

func (s *RelatedSymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte("../../nodes/" + s.nodeID), fs.OK
}

func (s *RelatedSymlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	target := "../../nodes/" + s.nodeID
	out.Mode = 0777 | syscall.S_IFLNK
	out.Size = uint64(len(target))
	return fs.OK
}
