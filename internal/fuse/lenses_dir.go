package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// LensesRootDir is the /lenses/ directory. Lists all nodes of type "Lens".
type LensesRootDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*LensesRootDir)(nil))
var _ = (fs.NodeReaddirer)((*LensesRootDir)(nil))
var _ = (fs.NodeGetattrer)((*LensesRootDir)(nil))

func (d *LensesRootDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("lenses")
	return fs.OK
}

func (d *LensesRootDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ids := d.repo.Search.FilterByType("Lens", 0)
	entries := make([]fuse.DirEntry, len(ids))
	for i, id := range ids {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("lenses/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *LensesRootDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Verify the node exists and is of type Lens
	node, err := d.repo.GetNode(name)
	if err != nil || node.Type != "Lens" {
		return nil, syscall.ENOENT
	}
	dir := &LensViewDir{repo: d.repo, lensID: name}
	child := d.NewInode(ctx, dir, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  stableIno("lenses/" + name),
	})
	return child, fs.OK
}

// LensViewDir is /lenses/{lens-id}/ — lists entities linked via INTERPRETED_THROUGH.
type LensViewDir struct {
	fs.Inode
	repo   *dag.Repository
	lensID string
}

var _ = (fs.NodeLookuper)((*LensViewDir)(nil))
var _ = (fs.NodeReaddirer)((*LensViewDir)(nil))
var _ = (fs.NodeGetattrer)((*LensViewDir)(nil))

func (d *LensViewDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("lenses/" + d.lensID)
	return fs.OK
}

// entities returns the node IDs that link to this lens via INTERPRETED_THROUGH.
func (d *LensViewDir) entities() []string {
	links := d.repo.Links.LinksTo(d.lensID)
	var ids []string
	for _, l := range links {
		if l.Type == "INTERPRETED_THROUGH" {
			ids = append(ids, l.Source)
		}
	}
	return ids
}

func (d *LensViewDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ids := d.entities()
	entries := make([]fuse.DirEntry, len(ids))
	for i, id := range ids {
		entries[i] = fuse.DirEntry{
			Name: id,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("lenses/" + d.lensID + "/" + id),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *LensViewDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	ids := d.entities()
	found := false
	for _, id := range ids {
		if id == name {
			found = true
			break
		}
	}
	if !found {
		return nil, syscall.ENOENT
	}

	sym := &LensSymlink{nodeID: name}
	child := d.NewInode(ctx, sym, fs.StableAttr{
		Mode: syscall.S_IFLNK,
		Ino:  stableIno("lenses/" + d.lensID + "/" + name),
	})
	return child, fs.OK
}

// LensSymlink points to ../../nodes/{id}.
type LensSymlink struct {
	fs.Inode
	nodeID string
}

var _ = (fs.NodeReadlinker)((*LensSymlink)(nil))
var _ = (fs.NodeGetattrer)((*LensSymlink)(nil))

func (s *LensSymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte("../../nodes/" + s.nodeID), fs.OK
}

func (s *LensSymlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	target := "../../nodes/" + s.nodeID
	out.Mode = 0777 | syscall.S_IFLNK
	out.Size = uint64(len(target))
	return fs.OK
}
