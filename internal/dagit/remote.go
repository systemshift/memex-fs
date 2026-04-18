package dagit

import (
	"encoding/json"
	"fmt"

	gocid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/systemshift/memex-fs/internal/dag"
)

// BlockCIDCodec and BlockMhType mirror the encoding memex-fs uses for its
// ObjectStore: CIDv1, raw codec (0x55), SHA2-256. Preserving these through
// IPFS block/put means the same CID survives a push/pull round-trip.
const (
	BlockCIDCodec = "raw"
	BlockMhType   = "sha2-256"
)

// kuboAPI is the subset of KuboClient that Push/Pull need. Extracting it
// lets the tests swap in a fake backend without standing up a real Kubo
// daemon.
type kuboAPI interface {
	BlockPut(data []byte, codec, mhType string) (string, error)
	BlockGet(cid string) ([]byte, error)
	Pin(cid string) error
}

// Push uploads every object reachable from HEAD (the commit, all ref node
// envelopes, and the `prev` chain of each node) to IPFS using block/put.
// Because block/put preserves the raw bytes and uses the same CID scheme
// as memex-fs, the returned HEAD CID is identical to repo.Commits.Head().
//
// After push, share the HEAD CID and any peer can `memex-fs pull <cid>`.
func Push(repo *dag.Repository, kubo kuboAPI) (string, error) {
	head, err := repo.Commits.Head()
	if err != nil {
		return "", fmt.Errorf("read HEAD: %w", err)
	}
	if head == dag.CidUndef {
		return "", fmt.Errorf("nothing to push: no commits yet")
	}

	pushed := make(map[string]bool)
	if err := pushObject(repo, kubo, head, pushed); err != nil {
		return "", err
	}

	commit, err := repo.Commits.GetCommit(head)
	if err != nil {
		return "", fmt.Errorf("load HEAD commit: %w", err)
	}
	if err := pushCommitRefs(repo, kubo, commit, pushed); err != nil {
		return "", err
	}
	if err := pushCommitParents(repo, kubo, commit, pushed); err != nil {
		return "", err
	}

	return dag.CIDToFilename(head), nil
}

// pushObject reads a single CID from the local ObjectStore and uploads it
// to IPFS via block/put. No-op if already pushed in this run. The local
// store is authoritative for bytes — if a CID isn't present, that's an
// error (corrupted repo), not a reason to skip.
func pushObject(repo *dag.Repository, kubo kuboAPI, c gocid.Cid, pushed map[string]bool) error {
	key := dag.CIDToFilename(c)
	if pushed[key] {
		return nil
	}
	data, err := repo.Store.Get(c)
	if err != nil {
		return fmt.Errorf("read local object %s: %w", key, err)
	}
	returned, err := kubo.BlockPut(data, BlockCIDCodec, BlockMhType)
	if err != nil {
		return fmt.Errorf("push %s: %w", key, err)
	}
	if returned != key {
		return fmt.Errorf("CID mismatch pushing %s: ipfs returned %s", key, returned)
	}
	pushed[key] = true
	return nil
}

// pushCommitRefs uploads every referenced node envelope, then recurses
// through each node's Prev pointer so the full history of each node is
// preserved (not just the latest version).
func pushCommitRefs(repo *dag.Repository, kubo kuboAPI, commit *dag.CommitObject, pushed map[string]bool) error {
	for _, cidStr := range commit.Refs {
		c, err := decodeCID(cidStr)
		if err != nil {
			return err
		}
		if err := pushNodeAndPrev(repo, kubo, c, pushed); err != nil {
			return err
		}
	}
	return nil
}

// pushNodeAndPrev walks a single node's version chain (newest to oldest)
// via NodeEnvelope.Prev, uploading each version.
func pushNodeAndPrev(repo *dag.Repository, kubo kuboAPI, c gocid.Cid, pushed map[string]bool) error {
	current := c
	for {
		if pushed[dag.CIDToFilename(current)] {
			return nil
		}
		data, err := repo.Store.Get(current)
		if err != nil {
			return fmt.Errorf("read node object: %w", err)
		}
		if _, err := kubo.BlockPut(data, BlockCIDCodec, BlockMhType); err != nil {
			return fmt.Errorf("push node: %w", err)
		}
		pushed[dag.CIDToFilename(current)] = true

		var node dag.NodeEnvelope
		if err := json.Unmarshal(data, &node); err != nil {
			// Non-fatal: if the object isn't a node envelope, we've pushed
			// its bytes and stop here.
			return nil
		}
		if node.Prev == "" {
			return nil
		}
		prev, err := decodeCID(node.Prev)
		if err != nil {
			return err
		}
		current = prev
	}
}

// pushCommitParents walks the commit chain backward and uploads each
// ancestor commit. Node refs from older commits are NOT re-pushed —
// the per-node Prev chains already covered them.
func pushCommitParents(repo *dag.Repository, kubo kuboAPI, commit *dag.CommitObject, pushed map[string]bool) error {
	parentStr := commit.Parent
	for parentStr != "" {
		c, err := decodeCID(parentStr)
		if err != nil {
			return err
		}
		if pushed[dag.CIDToFilename(c)] {
			return nil
		}
		if err := pushObject(repo, kubo, c, pushed); err != nil {
			return err
		}
		parent, err := repo.Commits.GetCommit(c)
		if err != nil {
			return fmt.Errorf("load parent commit: %w", err)
		}
		parentStr = parent.Parent
	}
	return nil
}

// Pull fetches a commit CID and every object reachable from it into the
// local ObjectStore. Does NOT update local refs or HEAD — the pulled
// snapshot is browsable via /at/{cid}/, and importing specific nodes is
// left as an explicit user action.
//
// If the same CID has been pulled before, the local ObjectStore already
// has the bytes and we skip the network call.
func Pull(repo *dag.Repository, kubo kuboAPI, headCIDStr string) error {
	head, err := decodeCID(headCIDStr)
	if err != nil {
		return fmt.Errorf("invalid CID: %w", err)
	}

	fetched := make(map[string]bool)
	if err := pullObject(repo, kubo, head, fetched); err != nil {
		return err
	}

	commit, err := repo.Commits.GetCommit(head)
	if err != nil {
		return fmt.Errorf("load pulled commit: %w", err)
	}
	if err := pullCommitRefs(repo, kubo, commit, fetched); err != nil {
		return err
	}
	if err := pullCommitParents(repo, kubo, commit, fetched); err != nil {
		return err
	}
	return nil
}

// pullObject fetches a single block from IPFS if not already local. After
// the network fetch, it re-Puts into the local store so the CID is
// canonicalized (and a disk-format consistency check happens).
func pullObject(repo *dag.Repository, kubo kuboAPI, c gocid.Cid, fetched map[string]bool) error {
	key := dag.CIDToFilename(c)
	if fetched[key] {
		return nil
	}
	if repo.Store.Has(c) {
		fetched[key] = true
		return nil
	}
	data, err := kubo.BlockGet(key)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", key, err)
	}
	gotCID, err := repo.Store.Put(data)
	if err != nil {
		return fmt.Errorf("store %s locally: %w", key, err)
	}
	if dag.CIDToFilename(gotCID) != key {
		return fmt.Errorf("CID mismatch on pull: asked %s, stored as %s", key, dag.CIDToFilename(gotCID))
	}
	// Best-effort pin so the remote doesn't GC out from under us.
	_ = kubo.Pin(key)
	fetched[key] = true
	return nil
}

func pullCommitRefs(repo *dag.Repository, kubo kuboAPI, commit *dag.CommitObject, fetched map[string]bool) error {
	for _, cidStr := range commit.Refs {
		c, err := decodeCID(cidStr)
		if err != nil {
			return err
		}
		if err := pullNodeAndPrev(repo, kubo, c, fetched); err != nil {
			return err
		}
	}
	return nil
}

func pullNodeAndPrev(repo *dag.Repository, kubo kuboAPI, c gocid.Cid, fetched map[string]bool) error {
	current := c
	for {
		if fetched[dag.CIDToFilename(current)] {
			return nil
		}
		if err := pullObject(repo, kubo, current, fetched); err != nil {
			return err
		}
		data, err := repo.Store.Get(current)
		if err != nil {
			return err
		}
		var node dag.NodeEnvelope
		if err := json.Unmarshal(data, &node); err != nil {
			return nil // not a node — we've fetched its bytes, done
		}
		if node.Prev == "" {
			return nil
		}
		prev, err := decodeCID(node.Prev)
		if err != nil {
			return err
		}
		current = prev
	}
}

func pullCommitParents(repo *dag.Repository, kubo kuboAPI, commit *dag.CommitObject, fetched map[string]bool) error {
	parentStr := commit.Parent
	for parentStr != "" {
		c, err := decodeCID(parentStr)
		if err != nil {
			return err
		}
		if fetched[dag.CIDToFilename(c)] {
			return nil
		}
		if err := pullObject(repo, kubo, c, fetched); err != nil {
			return err
		}
		parent, err := repo.Commits.GetCommit(c)
		if err != nil {
			return fmt.Errorf("load pulled parent: %w", err)
		}
		parentStr = parent.Parent
	}
	return nil
}

// decodeCID parses a base32-encoded CID string (as produced by CIDToFilename).
func decodeCID(s string) (gocid.Cid, error) {
	_, bytes, err := multibase.Decode(s)
	if err != nil {
		return gocid.Undef, fmt.Errorf("decode CID %s: %w", s, err)
	}
	c, err := gocid.Cast(bytes)
	if err != nil {
		return gocid.Undef, fmt.Errorf("parse CID %s: %w", s, err)
	}
	return c, nil
}
