package fuse

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// BlocksDir is /nodes/{id}/blocks/ — paragraph-level slices of the node's
// content, addressable as b0001, b0002, ... Read-only. Recomputed on each
// access, so edits to the parent's content immediately re-slice.
//
// Agents can reference a block externally by path (e.g.
// mount/nodes/person:alice/blocks/b0003). First-class block-to-block
// linking isn't wired yet; that would require the link system to accept
// compound IDs like "person:alice#b3".
type BlocksDir struct {
	fs.Inode
	repo   *dag.Repository
	nodeID string
}

var _ = (fs.NodeLookuper)((*BlocksDir)(nil))
var _ = (fs.NodeReaddirer)((*BlocksDir)(nil))
var _ = (fs.NodeGetattrer)((*BlocksDir)(nil))

func (d *BlocksDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0555
	out.Ino = stableIno("nodes/" + d.nodeID + "/blocks")
	return fs.OK
}

// blockName formats the 1-based index as a zero-padded, sortable filename.
func blockName(idx int) string {
	return fmt.Sprintf("b%04d", idx)
}

// parseBlockName is the inverse of blockName; returns the 1-based index, or
// 0 on malformed input.
func parseBlockName(name string) int {
	if !strings.HasPrefix(name, "b") {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimPrefix(name, "b"))
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func (d *BlocksDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	node, err := d.repo.GetNode(d.nodeID)
	if err != nil {
		return fs.NewListDirStream(nil), fs.OK
	}
	blocks := dag.Blocks(node.Content)
	entries := make([]fuse.DirEntry, len(blocks))
	for i, b := range blocks {
		name := blockName(b.Index)
		entries[i] = fuse.DirEntry{
			Name: name,
			Mode: syscall.S_IFREG,
			Ino:  stableIno("nodes/" + d.nodeID + "/blocks/" + name),
		}
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *BlocksDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	idx := parseBlockName(name)
	if idx == 0 {
		return nil, syscall.ENOENT
	}
	node, err := d.repo.GetNode(d.nodeID)
	if err != nil {
		return nil, syscall.ENOENT
	}
	blocks := dag.Blocks(node.Content)
	if idx > len(blocks) {
		return nil, syscall.ENOENT
	}
	child := d.NewInode(ctx, &BlockFile{
		repo:   d.repo,
		nodeID: d.nodeID,
		index:  idx,
	}, fs.StableAttr{
		Mode: syscall.S_IFREG,
		Ino:  stableIno("nodes/" + d.nodeID + "/blocks/" + name),
	})
	return child, fs.OK
}

// BlockFile is a single block. Read-only. Size and contents are derived on
// each access from the parent node's current content — so edits to the
// parent propagate without any notification machinery.
type BlockFile struct {
	fs.Inode
	repo   *dag.Repository
	nodeID string
	index  int
}

var _ = (fs.NodeGetattrer)((*BlockFile)(nil))
var _ = (fs.NodeOpener)((*BlockFile)(nil))
var _ = (fs.NodeReader)((*BlockFile)(nil))

func (f *BlockFile) text() []byte {
	node, err := f.repo.GetNode(f.nodeID)
	if err != nil {
		return nil
	}
	blocks := dag.Blocks(node.Content)
	if f.index <= 0 || f.index > len(blocks) {
		return nil
	}
	return append([]byte(blocks[f.index-1].Text), '\n')
}

func (f *BlockFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	data := f.text()
	if data == nil {
		return syscall.ENOENT
	}
	out.Mode = 0444
	out.Size = uint64(len(data))
	out.Ino = stableIno(fmt.Sprintf("nodes/%s/blocks/b%04d", f.nodeID, f.index))
	return fs.OK
}

func (f *BlockFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *BlockFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data := f.text()
	if data == nil {
		return nil, syscall.ENOENT
	}
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}
