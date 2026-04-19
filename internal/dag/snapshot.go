package dag

import (
	"encoding/json"
	"fmt"
	"time"

	gocid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
)

// Snapshot is a read-only view of the repository as of a specific commit.
// It mirrors Repository's read surface (GetNode, ListNodes, LinksFrom, LinksTo)
// without mutating state, so FUSE /at/{cid}/ can reuse the same shape.
//
// All lookups resolve through the commit's Refs map (id -> CID), so deleted
// or renamed nodes at later commits do not leak through.
type Snapshot struct {
	commit *CommitObject
	store  *ObjectStore

	forward map[string][]LinkEntry
	reverse map[string][]LinkEntry
}

// NewSnapshot builds a Snapshot from a resolved CommitObject. It pre-computes
// the forward/reverse link maps from the commit's link snapshot so the view
// is self-contained and does not depend on the live LinkIndex.
func NewSnapshot(commit *CommitObject, store *ObjectStore) *Snapshot {
	forward := make(map[string][]LinkEntry, len(commit.Links))
	reverse := make(map[string][]LinkEntry, len(commit.Links))
	for _, l := range commit.Links {
		forward[l.Source] = append(forward[l.Source], l)
		// Mirror LinkIndex: reverse is keyed by the parent node so
		// block-scoped targets surface as backlinks on the whole node.
		reverse[LinkTargetParent(l.Target)] = append(reverse[LinkTargetParent(l.Target)], l)
	}
	return &Snapshot{
		commit:  commit,
		store:   store,
		forward: forward,
		reverse: reverse,
	}
}

// Timestamp returns when this commit was made.
func (s *Snapshot) Timestamp() time.Time {
	return s.commit.Timestamp
}

// Message returns the commit message for this snapshot.
func (s *Snapshot) Message() string {
	return s.commit.Message
}

// GetNode resolves an id to its NodeEnvelope as it existed at this commit.
// Returns an error for deleted nodes so the FUSE layer can hide tombstones.
func (s *Snapshot) GetNode(id string) (*NodeEnvelope, error) {
	cidStr, ok := s.commit.Refs[id]
	if !ok {
		return nil, fmt.Errorf("node not in snapshot: %s", id)
	}
	_, cidBytes, err := multibase.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("decode ref CID %s: %w", cidStr, err)
	}
	c, err := gocid.Cast(cidBytes)
	if err != nil {
		return nil, fmt.Errorf("parse ref CID: %w", err)
	}
	data, err := s.store.Get(c)
	if err != nil {
		return nil, err
	}
	var node NodeEnvelope
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("unmarshal node: %w", err)
	}
	if node.Deleted {
		return nil, fmt.Errorf("node deleted: %s", id)
	}
	return &node, nil
}

// ListNodes returns all non-deleted node IDs in this snapshot.
func (s *Snapshot) ListNodes() []string {
	ids := make([]string, 0, len(s.commit.Refs))
	for id := range s.commit.Refs {
		node, err := s.GetNode(id)
		if err != nil || node == nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// LinksFrom returns outgoing links for a node as of this commit.
func (s *Snapshot) LinksFrom(id string) []LinkEntry {
	return s.forward[id]
}

// LinksTo returns incoming links for a node as of this commit, including
// links whose target is a block of this node (target="id#b{n}").
func (s *Snapshot) LinksTo(id string) []LinkEntry {
	return s.reverse[LinkTargetParent(id)]
}
