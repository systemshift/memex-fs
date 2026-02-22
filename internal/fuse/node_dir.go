package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

// NodeDir represents a single node directory (e.g. nodes/person:alice/).
// Contains: content, meta.json, type, links/
type NodeDir struct {
	fs.Inode
	repo      *dag.Repository
	nodeID    string
	accessLog *AccessLog
}

var _ = (fs.NodeLookuper)((*NodeDir)(nil))
var _ = (fs.NodeReaddirer)((*NodeDir)(nil))
var _ = (fs.NodeGetattrer)((*NodeDir)(nil))

func (d *NodeDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("nodes/" + d.nodeID)
	return fs.OK
}

func (d *NodeDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "content", Mode: syscall.S_IFREG, Ino: stableIno("nodes/" + d.nodeID + "/content")},
		{Name: "meta.json", Mode: syscall.S_IFREG, Ino: stableIno("nodes/" + d.nodeID + "/meta.json")},
		{Name: "type", Mode: syscall.S_IFREG, Ino: stableIno("nodes/" + d.nodeID + "/type")},
		{Name: "links", Mode: syscall.S_IFDIR, Ino: stableIno("nodes/" + d.nodeID + "/links")},
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *NodeDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "content":
		f := &ContentFile{repo: d.repo, nodeID: d.nodeID, accessLog: d.accessLog}
		child := d.NewInode(ctx, f, fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  stableIno("nodes/" + d.nodeID + "/content"),
		})
		return child, fs.OK

	case "meta.json":
		f := &MetaFile{repo: d.repo, nodeID: d.nodeID, accessLog: d.accessLog}
		child := d.NewInode(ctx, f, fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  stableIno("nodes/" + d.nodeID + "/meta.json"),
		})
		return child, fs.OK

	case "type":
		f := &TypeFile{repo: d.repo, nodeID: d.nodeID, accessLog: d.accessLog}
		child := d.NewInode(ctx, f, fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  stableIno("nodes/" + d.nodeID + "/type"),
		})
		return child, fs.OK

	case "links":
		f := &LinksDir{repo: d.repo, nodeID: d.nodeID, accessLog: d.accessLog}
		child := d.NewInode(ctx, f, fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Ino:  stableIno("nodes/" + d.nodeID + "/links"),
		})
		return child, fs.OK

	default:
		return nil, syscall.ENOENT
	}
}

// ContentFile exposes a node's content as a readable/writable file.
type ContentFile struct {
	fs.Inode
	repo      *dag.Repository
	nodeID    string
	accessLog *AccessLog
}

var _ = (fs.NodeGetattrer)((*ContentFile)(nil))
var _ = (fs.NodeSetattrer)((*ContentFile)(nil))
var _ = (fs.NodeOpener)((*ContentFile)(nil))
var _ = (fs.NodeReader)((*ContentFile)(nil))

func (f *ContentFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	node, err := f.repo.GetNode(f.nodeID)
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = 0644
	out.Size = uint64(len(node.Content))
	out.Ino = stableIno("nodes/" + f.nodeID + "/content")
	return fs.OK
}

func (f *ContentFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// Accept truncation — the actual write will happen via WriteHandle
	return f.Getattr(ctx, fh, out)
}

func (f *ContentFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 || flags&syscall.O_TRUNC != 0 {
		wh := &WriteHandle{
			repo:   f.repo,
			nodeID: f.nodeID,
			field:  "content",
		}
		return wh, fuse.FOPEN_DIRECT_IO, fs.OK
	}
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *ContentFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	node, err := f.repo.GetNode(f.nodeID)
	if err != nil {
		return nil, syscall.ENOENT
	}
	if f.accessLog != nil {
		f.accessLog.Log(f.nodeID, "content")
	}
	data := node.Content
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

// MetaFile exposes a node's metadata as JSON.
type MetaFile struct {
	fs.Inode
	repo      *dag.Repository
	nodeID    string
	accessLog *AccessLog
}

var _ = (fs.NodeGetattrer)((*MetaFile)(nil))
var _ = (fs.NodeSetattrer)((*MetaFile)(nil))
var _ = (fs.NodeOpener)((*MetaFile)(nil))
var _ = (fs.NodeReader)((*MetaFile)(nil))

func (f *MetaFile) metaBytes() ([]byte, error) {
	node, err := f.repo.GetNode(f.nodeID)
	if err != nil {
		return nil, err
	}
	m := node.Meta
	if m == nil {
		m = make(map[string]interface{})
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (f *MetaFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	data, err := f.metaBytes()
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = 0644
	out.Size = uint64(len(data))
	out.Ino = stableIno("nodes/" + f.nodeID + "/meta.json")
	return fs.OK
}

func (f *MetaFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return f.Getattr(ctx, fh, out)
}

func (f *MetaFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 || flags&syscall.O_TRUNC != 0 {
		wh := &WriteHandle{
			repo:   f.repo,
			nodeID: f.nodeID,
			field:  "meta",
		}
		return wh, fuse.FOPEN_DIRECT_IO, fs.OK
	}
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *MetaFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data, err := f.metaBytes()
	if err != nil {
		return nil, syscall.ENOENT
	}
	if f.accessLog != nil {
		f.accessLog.Log(f.nodeID, "meta")
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

// TypeFile exposes a node's type as a read-only file.
type TypeFile struct {
	fs.Inode
	repo      *dag.Repository
	nodeID    string
	accessLog *AccessLog
}

var _ = (fs.NodeGetattrer)((*TypeFile)(nil))
var _ = (fs.NodeOpener)((*TypeFile)(nil))
var _ = (fs.NodeReader)((*TypeFile)(nil))

func (f *TypeFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	node, err := f.repo.GetNode(f.nodeID)
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = 0444
	out.Size = uint64(len(node.Type) + 1)
	out.Ino = stableIno("nodes/" + f.nodeID + "/type")
	return fs.OK
}

func (f *TypeFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *TypeFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	node, err := f.repo.GetNode(f.nodeID)
	if err != nil {
		return nil, syscall.ENOENT
	}
	if f.accessLog != nil {
		f.accessLog.Log(f.nodeID, "type")
	}
	data := []byte(node.Type + "\n")
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

// LinksDir lists links from a node as symlinks.
type LinksDir struct {
	fs.Inode
	repo      *dag.Repository
	nodeID    string
	accessLog *AccessLog
}

var _ = (fs.NodeLookuper)((*LinksDir)(nil))
var _ = (fs.NodeReaddirer)((*LinksDir)(nil))
var _ = (fs.NodeGetattrer)((*LinksDir)(nil))
var _ = (fs.NodeSymlinker)((*LinksDir)(nil))

func (d *LinksDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("nodes/" + d.nodeID + "/links")
	return fs.OK
}

func (d *LinksDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if d.accessLog != nil {
		d.accessLog.Log(d.nodeID, "links")
	}
	links := d.repo.GetLinks(d.nodeID)
	var entries []fuse.DirEntry
	for _, l := range links {
		// Only show outgoing links (source = this node)
		if l.Source != d.nodeID {
			continue
		}
		name := l.Type + ":" + l.Target
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: syscall.S_IFLNK,
			Ino:  stableIno("nodes/" + d.nodeID + "/links/" + name),
		})
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *LinksDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// name format: "linktype:targetid" e.g. "knows:person:bob"
	// Parse: first segment before ":" is link type, rest is target
	idx := strings.Index(name, ":")
	if idx < 0 {
		return nil, syscall.ENOENT
	}
	linkType := name[:idx]
	target := name[idx+1:]

	// Verify this link exists
	links := d.repo.Links.LinksFrom(d.nodeID)
	found := false
	for _, l := range links {
		if l.Type == linkType && l.Target == target {
			found = true
			break
		}
	}
	if !found {
		return nil, syscall.ENOENT
	}

	sym := &LinkSymlink{target: "../../" + target}
	child := d.NewInode(ctx, sym, fs.StableAttr{
		Mode: syscall.S_IFLNK,
		Ino:  stableIno("nodes/" + d.nodeID + "/links/" + name),
	})
	return child, fs.OK
}

func (d *LinksDir) Symlink(ctx context.Context, pointedTo string, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// name format: "linktype:targetid"
	idx := strings.Index(name, ":")
	if idx < 0 {
		return nil, syscall.EINVAL
	}
	linkType := name[:idx]
	target := name[idx+1:]

	// Verify the target node exists
	if _, err := d.repo.GetNode(target); err != nil {
		return nil, syscall.ENOENT
	}

	if err := d.repo.CreateLink(d.nodeID, target, linkType); err != nil {
		return nil, syscall.EIO
	}

	sym := &LinkSymlink{target: pointedTo}
	child := d.NewInode(ctx, sym, fs.StableAttr{
		Mode: syscall.S_IFLNK,
		Ino:  stableIno("nodes/" + d.nodeID + "/links/" + name),
	})
	return child, fs.OK
}

// LinkSymlink is a single symlink in the links/ directory.
type LinkSymlink struct {
	fs.Inode
	target string
}

var _ = (fs.NodeReadlinker)((*LinkSymlink)(nil))
var _ = (fs.NodeGetattrer)((*LinkSymlink)(nil))

func (s *LinkSymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte(s.target), fs.OK
}

func (s *LinkSymlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0777 | syscall.S_IFLNK
	out.Size = uint64(len(s.target))
	return fs.OK
}

// WriteHandle buffers writes and commits on flush/release.
type WriteHandle struct {
	repo   *dag.Repository
	nodeID string
	field  string // "content" or "meta"
	buf    []byte
}

const maxWriteSize = 64 << 20 // 64 MB

var _ = (fs.FileWriter)((*WriteHandle)(nil))
var _ = (fs.FileFlusher)((*WriteHandle)(nil))

func (h *WriteHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	end := int(off) + len(data)
	if end > maxWriteSize {
		return 0, syscall.EFBIG
	}
	// Extend buffer if needed
	if end > len(h.buf) {
		newBuf := make([]byte, end)
		copy(newBuf, h.buf)
		h.buf = newBuf
	}
	copy(h.buf[off:], data)
	return uint32(len(data)), fs.OK
}

func (h *WriteHandle) Flush(ctx context.Context) syscall.Errno {
	if h.buf == nil {
		return fs.OK
	}

	switch h.field {
	case "content":
		_, err := h.repo.UpdateContent(h.nodeID, h.buf)
		if err != nil {
			fmt.Printf("memex-fs: write content %s: %v\n", h.nodeID, err)
			return syscall.EIO
		}
	case "meta":
		var meta map[string]interface{}
		if err := json.Unmarshal(h.buf, &meta); err != nil {
			fmt.Printf("memex-fs: invalid meta JSON for %s: %v\n", h.nodeID, err)
			return syscall.EINVAL
		}
		_, err := h.repo.UpdateNode(h.nodeID, meta)
		if err != nil {
			fmt.Printf("memex-fs: write meta %s: %v\n", h.nodeID, err)
			return syscall.EIO
		}
	}
	return fs.OK
}
