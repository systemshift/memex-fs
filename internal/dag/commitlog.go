package dag

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	gocid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
)

// CommitLog manages the commit chain. HEAD is stored as a single-line file at .mx/HEAD.
type CommitLog struct {
	headPath string
	store    *ObjectStore
	author   string // DID of the local identity, stamped on every commit
}

// NewCommitLog creates a CommitLog that reads/writes HEAD from headPath.
func NewCommitLog(headPath string, store *ObjectStore, author string) *CommitLog {
	return &CommitLog{headPath: headPath, store: store, author: author}
}

// Head returns the CID of the current HEAD commit, or gocid.Undef if none.
func (cl *CommitLog) Head() (gocid.Cid, error) {
	data, err := os.ReadFile(cl.headPath)
	if os.IsNotExist(err) {
		return gocid.Undef, nil
	}
	if err != nil {
		return gocid.Undef, fmt.Errorf("read HEAD: %w", err)
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		return gocid.Undef, nil
	}
	_, cidBytes, err := multibase.Decode(s)
	if err != nil {
		return gocid.Undef, fmt.Errorf("decode HEAD CID: %w", err)
	}
	return gocid.Cast(cidBytes)
}

// Commit creates a new commit object from the current state of refs and links.
// Returns the CID of the new commit.
func (cl *CommitLog) Commit(refs *RefStore, links *LinkIndex, message string) (gocid.Cid, error) {
	// 1. Snapshot refs: id → base32 CID
	ids, err := refs.List()
	if err != nil {
		return gocid.Undef, fmt.Errorf("list refs: %w", err)
	}
	sort.Strings(ids)
	refsMap := make(map[string]string, len(ids))
	for _, id := range ids {
		c, err := refs.Get(id)
		if err != nil {
			continue
		}
		refsMap[id] = CIDToFilename(c)
	}

	// 2. Snapshot links, sorted by source+target+type
	allLinks := links.AllEntries()
	sort.Slice(allLinks, func(i, j int) bool {
		if allLinks[i].Source != allLinks[j].Source {
			return allLinks[i].Source < allLinks[j].Source
		}
		if allLinks[i].Target != allLinks[j].Target {
			return allLinks[i].Target < allLinks[j].Target
		}
		return allLinks[i].Type < allLinks[j].Type
	})

	// 3. Read current HEAD as parent
	parent := ""
	head, err := cl.Head()
	if err == nil && head != gocid.Undef {
		parent = CIDToFilename(head)
	}

	// 4. Build commit object
	commit := &CommitObject{
		V:         1,
		Parent:    parent,
		Author:    cl.author,
		Timestamp: time.Now().UTC(),
		Refs:      refsMap,
		Links:     allLinks,
		Message:   message,
	}

	// 5. Serialize and store
	data, err := CanonicalJSON(commit)
	if err != nil {
		return gocid.Undef, fmt.Errorf("serialize commit: %w", err)
	}
	c, err := cl.store.Put(data)
	if err != nil {
		return gocid.Undef, fmt.Errorf("store commit: %w", err)
	}

	// 6. Update HEAD
	encoded := CIDToFilename(c)
	if err := SafeWrite(cl.headPath, []byte(encoded+"\n"), 0644); err != nil {
		return gocid.Undef, fmt.Errorf("write HEAD: %w", err)
	}

	return c, nil
}

// GetCommit reads and unmarshals a commit by CID.
func (cl *CommitLog) GetCommit(c gocid.Cid) (*CommitObject, error) {
	data, err := cl.store.Get(c)
	if err != nil {
		return nil, err
	}
	var commit CommitObject
	if err := json.Unmarshal(data, &commit); err != nil {
		return nil, fmt.Errorf("unmarshal commit: %w", err)
	}
	return &commit, nil
}

// Log walks the parent chain from HEAD, returning up to n commits (newest first).
func (cl *CommitLog) Log(n int) ([]CommitObject, error) {
	head, err := cl.Head()
	if err != nil || head == gocid.Undef {
		return nil, err
	}

	var commits []CommitObject
	current := head
	for i := 0; i < n && current != gocid.Undef; i++ {
		commit, err := cl.GetCommit(current)
		if err != nil {
			break
		}
		commits = append(commits, *commit)

		// Follow parent
		if commit.Parent == "" {
			break
		}
		_, cidBytes, err := multibase.Decode(commit.Parent)
		if err != nil {
			break
		}
		current, err = gocid.Cast(cidBytes)
		if err != nil {
			break
		}
	}
	return commits, nil
}
