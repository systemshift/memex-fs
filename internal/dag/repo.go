package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	coAccessWindow = 5 * time.Minute
	coChangeWindow = 1 * time.Hour
)

// Repository is the top-level facade for the Merkle DAG store.
type Repository struct {
	root        string
	Store       *ObjectStore
	Refs        *RefStore
	Links       *LinkIndex
	Search      *SearchIndex
	Commits     *CommitLog
	CoAccess    *CoAccessIndex
	CoChange    *CoChangeIndex
	Relatedness *RelatednessIndex
}

// OpenRepository opens or creates a repository at the given path.
func OpenRepository(root string) (*Repository, error) {
	mxDir := filepath.Join(root, ".mx")

	// Ensure directory structure
	for _, dir := range []string{
		mxDir,
		filepath.Join(mxDir, "objects"),
		filepath.Join(mxDir, "refs"),
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create dir %s: %w", dir, err)
		}
	}

	// Create meta.json if it doesn't exist
	metaPath := filepath.Join(mxDir, "meta.json")
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		meta := map[string]interface{}{
			"version": 1,
			"created": time.Now().UTC().Format(time.RFC3339),
		}
		data, _ := json.MarshalIndent(meta, "", "  ")
		os.WriteFile(metaPath, data, 0644)
	}

	store, err := NewObjectStore(filepath.Join(mxDir, "objects"))
	if err != nil {
		return nil, err
	}

	refs, err := NewRefStore(filepath.Join(mxDir, "refs"))
	if err != nil {
		return nil, err
	}

	links, err := NewLinkIndex(filepath.Join(mxDir, "links.jsonl"))
	if err != nil {
		return nil, err
	}

	search := NewSearchIndex()

	// Load shared identity for commit authorship
	author := ""
	if id, err := LoadIdentity(); err != nil {
		fmt.Printf("memex-fs: identity warning: %v\n", err)
	} else {
		author = id.DID
	}

	commits := NewCommitLog(filepath.Join(mxDir, "HEAD"), store, author)

	// Build advisory indexes (failures are warnings, not fatal)
	accessLogPath := filepath.Join(mxDir, "access.jsonl")
	coAccess := NewCoAccessIndex(accessLogPath, coAccessWindow)

	coChange := NewCoChangeIndex(commits, coChangeWindow)
	coChange.Build()

	relatedness := NewRelatednessIndex(coAccess, coChange)

	repo := &Repository{
		root:        root,
		Store:       store,
		Refs:        refs,
		Links:       links,
		Search:      search,
		Commits:     commits,
		CoAccess:    coAccess,
		CoChange:    coChange,
		Relatedness: relatedness,
	}

	// Rebuild search index from all refs
	if err := repo.rebuildSearchIndex(); err != nil {
		return nil, fmt.Errorf("rebuild search index: %w", err)
	}

	return repo, nil
}

// MxDir returns the path to the .mx/ data directory.
func (r *Repository) MxDir() string {
	return filepath.Join(r.root, ".mx")
}

// commit is a helper that creates a commit after a mutation.
// Failures are logged but do not propagate — commits are metadata, not essential.
func (r *Repository) commit(message string) {
	if _, err := r.Commits.Commit(r.Refs, r.Links, message); err != nil {
		fmt.Printf("memex-fs: commit warning: %v\n", err)
	}
}

// rebuildSearchIndex scans all refs and indexes every node.
func (r *Repository) rebuildSearchIndex() error {
	ids, err := r.Refs.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		node, err := r.getNodeEnvelope(id)
		if err != nil {
			continue // skip broken refs
		}
		if !node.Deleted {
			r.Search.IndexNode(id, node)
		}
	}
	return nil
}

// getNodeEnvelope resolves a ref to its NodeEnvelope.
func (r *Repository) getNodeEnvelope(id string) (*NodeEnvelope, error) {
	c, err := r.Refs.Get(id)
	if err != nil {
		return nil, err
	}
	data, err := r.Store.Get(c)
	if err != nil {
		return nil, err
	}
	var node NodeEnvelope
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("unmarshal node: %w", err)
	}
	return &node, nil
}

// CreateNode creates a new node and stores it.
func (r *Repository) CreateNode(id, typ string, content []byte, meta map[string]interface{}) (*NodeEnvelope, error) {
	now := time.Now().UTC()
	node := &NodeEnvelope{
		V:        1,
		ID:       id,
		Type:     typ,
		Content:  content,
		Meta:     meta,
		Created:  now,
		Modified: now,
	}

	data, err := CanonicalJSON(node)
	if err != nil {
		return nil, fmt.Errorf("serialize node: %w", err)
	}

	c, err := r.Store.Put(data)
	if err != nil {
		return nil, fmt.Errorf("store object: %w", err)
	}

	if err := r.Refs.Set(id, c); err != nil {
		return nil, fmt.Errorf("set ref: %w", err)
	}

	r.Search.IndexNode(id, node)
	r.commit("create " + id)
	return node, nil
}

// GetNode retrieves a node by its human-readable ID.
func (r *Repository) GetNode(id string) (*NodeEnvelope, error) {
	node, err := r.getNodeEnvelope(id)
	if err != nil {
		return nil, err
	}
	if node.Deleted {
		return nil, fmt.Errorf("node deleted: %s", id)
	}
	return node, nil
}

// ListNodes returns all non-deleted node IDs with optional limit.
func (r *Repository) ListNodes(limit int) ([]string, error) {
	ids, err := r.Refs.List()
	if err != nil {
		return nil, err
	}
	// Filter out deleted nodes
	var result []string
	for _, id := range ids {
		node, err := r.getNodeEnvelope(id)
		if err != nil || node.Deleted {
			continue
		}
		result = append(result, id)
	}
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// UpdateNode patches a node's metadata, creating a new version.
func (r *Repository) UpdateNode(id string, metaUpdates map[string]interface{}) (*NodeEnvelope, error) {
	current, err := r.getNodeEnvelope(id)
	if err != nil {
		return nil, err
	}
	if current.Deleted {
		return nil, fmt.Errorf("cannot update deleted node: %s", id)
	}

	// Get current CID for prev pointer
	prevCID, _ := r.Refs.Get(id)

	// Merge metadata
	if current.Meta == nil {
		current.Meta = make(map[string]interface{})
	}
	for k, v := range metaUpdates {
		if v == nil {
			delete(current.Meta, k)
		} else {
			current.Meta[k] = v
		}
	}

	now := time.Now().UTC()
	node := &NodeEnvelope{
		V:        1,
		ID:       id,
		Type:     current.Type,
		Content:  current.Content,
		Meta:     current.Meta,
		Created:  current.Created,
		Modified: now,
		Prev:     CIDToFilename(prevCID),
	}

	data, err := CanonicalJSON(node)
	if err != nil {
		return nil, fmt.Errorf("serialize node: %w", err)
	}

	c, err := r.Store.Put(data)
	if err != nil {
		return nil, fmt.Errorf("store object: %w", err)
	}

	if err := r.Refs.Set(id, c); err != nil {
		return nil, fmt.Errorf("update ref: %w", err)
	}

	r.Search.RemoveNode(id)
	r.Search.IndexNode(id, node)
	r.commit("update meta " + id)
	return node, nil
}

// DeleteNode soft-deletes a node by creating a tombstone.
func (r *Repository) DeleteNode(id string, force bool) error {
	if force {
		// Hard delete: just remove the ref
		r.Search.RemoveNode(id)
		if err := r.Refs.Delete(id); err != nil {
			return err
		}
		r.commit("delete " + id)
		return nil
	}

	current, err := r.getNodeEnvelope(id)
	if err != nil {
		return err
	}

	prevCID, _ := r.Refs.Get(id)

	tombstone := &NodeEnvelope{
		V:        1,
		ID:       id,
		Type:     current.Type,
		Meta:     current.Meta,
		Created:  current.Created,
		Modified: time.Now().UTC(),
		Prev:     CIDToFilename(prevCID),
		Deleted:  true,
	}

	data, err := CanonicalJSON(tombstone)
	if err != nil {
		return fmt.Errorf("serialize tombstone: %w", err)
	}

	c, err := r.Store.Put(data)
	if err != nil {
		return fmt.Errorf("store tombstone: %w", err)
	}

	if err := r.Refs.Set(id, c); err != nil {
		return fmt.Errorf("update ref: %w", err)
	}

	r.Search.RemoveNode(id)
	r.commit("delete " + id)
	return nil
}

// UpdateContent replaces a node's content, creating a new version.
func (r *Repository) UpdateContent(id string, content []byte) (*NodeEnvelope, error) {
	current, err := r.getNodeEnvelope(id)
	if err != nil {
		return nil, err
	}
	if current.Deleted {
		return nil, fmt.Errorf("cannot update deleted node: %s", id)
	}

	prevCID, _ := r.Refs.Get(id)

	now := time.Now().UTC()
	node := &NodeEnvelope{
		V:        1,
		ID:       id,
		Type:     current.Type,
		Content:  content,
		Meta:     current.Meta,
		Created:  current.Created,
		Modified: now,
		Prev:     CIDToFilename(prevCID),
	}

	data, err := CanonicalJSON(node)
	if err != nil {
		return nil, fmt.Errorf("serialize node: %w", err)
	}

	c, err := r.Store.Put(data)
	if err != nil {
		return nil, fmt.Errorf("store object: %w", err)
	}

	if err := r.Refs.Set(id, c); err != nil {
		return nil, fmt.Errorf("update ref: %w", err)
	}

	r.Search.RemoveNode(id)
	r.Search.IndexNode(id, node)
	r.commit("update content " + id)
	return node, nil
}

// CreateLink creates a link between two nodes.
func (r *Repository) CreateLink(source, target, linkType string) error {
	if err := r.Links.Add(LinkEntry{Source: source, Target: target, Type: linkType}); err != nil {
		return err
	}
	r.commit(fmt.Sprintf("link %s -[%s]-> %s", source, linkType, target))
	return nil
}

// GetLinks returns all links involving the given node.
func (r *Repository) GetLinks(id string) []LinkEntry {
	return r.Links.AllLinks(id)
}

// Ingest content-addresses raw content and creates a Source node.
func (r *Repository) Ingest(content string, format string) (string, bool, error) {
	hash := sha256.Sum256([]byte(content))
	hexHash := hex.EncodeToString(hash[:])
	id := "sha256:" + hexHash

	// Check for dedup
	if r.Refs.Has(id) {
		return id, false, nil // already exists
	}

	meta := map[string]interface{}{
		"format":     format,
		"size_bytes": len(content),
	}

	_, err := r.CreateNode(id, "Source", []byte(content), meta)
	if err != nil {
		return "", false, err
	}
	// CreateNode already commits, but with "create {id}" — that's fine for ingest too
	return id, true, nil
}

// SearchNodes searches the index and returns full nodes.
func (r *Repository) SearchNodes(query string, limit int) ([]*NodeEnvelope, error) {
	ids := r.Search.Search(query, limit)
	var nodes []*NodeEnvelope
	for _, id := range ids {
		node, err := r.GetNode(id)
		if err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// FilterNodes returns nodes matching a type filter.
func (r *Repository) FilterNodes(typ string, limit int) ([]*NodeEnvelope, error) {
	ids := r.Search.FilterByType(typ, limit)
	var nodes []*NodeEnvelope
	for _, id := range ids {
		node, err := r.GetNode(id)
		if err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// Traverse does a BFS from a start node to the given depth.
func (r *Repository) Traverse(startID string, depth int) ([]*NodeEnvelope, error) {
	visited := make(map[string]bool)
	queue := []string{startID}
	visited[startID] = true

	for d := 0; d < depth && len(queue) > 0; d++ {
		var next []string
		for _, id := range queue {
			links := r.Links.AllLinks(id)
			for _, l := range links {
				neighbor := l.Target
				if neighbor == id {
					neighbor = l.Source
				}
				if !visited[neighbor] {
					visited[neighbor] = true
					next = append(next, neighbor)
				}
			}
		}
		queue = next
	}

	var nodes []*NodeEnvelope
	for id := range visited {
		node, err := r.GetNode(id)
		if err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}
