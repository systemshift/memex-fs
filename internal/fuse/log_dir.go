package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/systemshift/memex-fs/internal/dag"
)

const maxLogEntries = 64

// LogDir exposes recent commits as files in the FUSE tree.
// Layout: log/HEAD (CID string), log/0 (newest commit JSON), log/1, ...
type LogDir struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeLookuper)((*LogDir)(nil))
var _ = (fs.NodeReaddirer)((*LogDir)(nil))
var _ = (fs.NodeGetattrer)((*LogDir)(nil))

func (d *LogDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Ino = stableIno("log")
	return fs.OK
}

func (d *LogDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "HEAD", Mode: syscall.S_IFREG, Ino: stableIno("log/HEAD")},
	}
	commits, _ := d.repo.Commits.Log(maxLogEntries)
	for i := range commits {
		name := fmt.Sprintf("%d", i)
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: syscall.S_IFREG,
			Ino:  stableIno("log/" + name),
		})
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (d *LogDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "HEAD" {
		f := &LogHeadFile{repo: d.repo}
		child := d.NewInode(ctx, f, fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  stableIno("log/HEAD"),
		})
		return child, fs.OK
	}

	// Parse index
	var idx int
	if _, err := fmt.Sscanf(name, "%d", &idx); err != nil || idx < 0 {
		return nil, syscall.ENOENT
	}

	commits, _ := d.repo.Commits.Log(idx + 1)
	if idx >= len(commits) {
		return nil, syscall.ENOENT
	}

	f := &LogEntryFile{commit: &commits[idx], name: name}
	child := d.NewInode(ctx, f, fs.StableAttr{
		Mode: syscall.S_IFREG,
		Ino:  stableIno("log/" + name),
	})
	return child, fs.OK
}

// LogHeadFile returns the HEAD CID string.
type LogHeadFile struct {
	fs.Inode
	repo *dag.Repository
}

var _ = (fs.NodeGetattrer)((*LogHeadFile)(nil))
var _ = (fs.NodeReader)((*LogHeadFile)(nil))
var _ = (fs.NodeOpener)((*LogHeadFile)(nil))

func (f *LogHeadFile) headBytes() []byte {
	head, err := f.repo.Commits.Head()
	if err != nil || head == dag.CidUndef {
		return []byte("(none)\n")
	}
	return []byte(dag.CIDToFilename(head) + "\n")
}

func (f *LogHeadFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0444
	out.Size = uint64(len(f.headBytes()))
	out.Ino = stableIno("log/HEAD")
	return fs.OK
}

func (f *LogHeadFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *LogHeadFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data := f.headBytes()
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}

// LogEntryFile returns indented JSON for a single commit.
type LogEntryFile struct {
	fs.Inode
	commit *dag.CommitObject
	name   string
}

var _ = (fs.NodeGetattrer)((*LogEntryFile)(nil))
var _ = (fs.NodeReader)((*LogEntryFile)(nil))
var _ = (fs.NodeOpener)((*LogEntryFile)(nil))

func (f *LogEntryFile) commitBytes() []byte {
	data, _ := json.MarshalIndent(f.commit, "", "  ")
	return append(data, '\n')
}

func (f *LogEntryFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0444
	out.Size = uint64(len(f.commitBytes()))
	out.Ino = stableIno("log/" + f.name)
	return fs.OK
}

func (f *LogEntryFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *LogEntryFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data := f.commitBytes()
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return fuse.ReadResultData(data[off:end]), fs.OK
}
