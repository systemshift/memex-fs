package dag

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

// LinkTargetParent returns the whole-node portion of a possibly-block-scoped
// link target. Block targets use the suffix "#b{n}" — e.g. "paper:abc#b3"
// is the third block of paper:abc. Backlink queries are indexed by this
// parent so a link into a block surfaces as a backlink on the node.
func LinkTargetParent(target string) string {
	if i := strings.Index(target, "#"); i > 0 {
		return target[:i]
	}
	return target
}

// LinkEntry is a single link record in the JSONL journal.
type LinkEntry struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"`
}

// LinkIndex maintains an append-only JSONL journal and in-memory forward/reverse maps.
type LinkIndex struct {
	mu      sync.RWMutex
	path    string
	forward map[string][]LinkEntry // source -> links
	reverse map[string][]LinkEntry // target -> links
}

// NewLinkIndex creates a LinkIndex, loading existing entries from the journal file.
func NewLinkIndex(path string) (*LinkIndex, error) {
	idx := &LinkIndex{
		path:    path,
		forward: make(map[string][]LinkEntry),
		reverse: make(map[string][]LinkEntry),
	}
	if err := idx.load(); err != nil {
		return nil, err
	}
	return idx, nil
}

func (idx *LinkIndex) load() error {
	f, err := os.Open(idx.path)
	if os.IsNotExist(err) {
		return nil // no journal yet
	}
	if err != nil {
		return fmt.Errorf("open link journal: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var entry LinkEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue // skip malformed lines
		}
		idx.forward[entry.Source] = append(idx.forward[entry.Source], entry)
		// Reverse map is keyed by the parent node so that block-scoped
		// targets surface as backlinks on the whole node.
		idx.reverse[LinkTargetParent(entry.Target)] = append(idx.reverse[LinkTargetParent(entry.Target)], entry)
	}
	return scanner.Err()
}

// Add appends a link to the journal and updates in-memory indexes.
func (idx *LinkIndex) Add(entry LinkEntry) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check for duplicate
	for _, existing := range idx.forward[entry.Source] {
		if existing.Target == entry.Target && existing.Type == entry.Type {
			return nil // already exists
		}
	}

	// Append to journal
	data, _ := json.Marshal(entry)
	if err := SafeAppend(idx.path, append(data, '\n')); err != nil {
		return fmt.Errorf("write link entry: %w", err)
	}

	idx.forward[entry.Source] = append(idx.forward[entry.Source], entry)
	idx.reverse[LinkTargetParent(entry.Target)] = append(idx.reverse[LinkTargetParent(entry.Target)], entry)
	return nil
}

// LinksFrom returns all links where the given ID is the source.
func (idx *LinkIndex) LinksFrom(id string) []LinkEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.forward[id]
}

// LinksTo returns all links whose target resolves to id — i.e. target is
// literally id, or a block-scoped target (id#b{n}) whose parent is id.
func (idx *LinkIndex) LinksTo(id string) []LinkEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.reverse[LinkTargetParent(id)]
}

// AllLinks returns all links involving the given ID (as source or target).
func (idx *LinkIndex) AllLinks(id string) []LinkEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	seen := make(map[string]bool)
	var result []LinkEntry
	for _, l := range idx.forward[id] {
		key := l.Source + "|" + l.Target + "|" + l.Type
		if !seen[key] {
			seen[key] = true
			result = append(result, l)
		}
	}
	for _, l := range idx.reverse[id] {
		key := l.Source + "|" + l.Target + "|" + l.Type
		if !seen[key] {
			seen[key] = true
			result = append(result, l)
		}
	}
	return result
}

// AllEntries returns every link in the index.
func (idx *LinkIndex) AllEntries() []LinkEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var result []LinkEntry
	for _, links := range idx.forward {
		result = append(result, links...)
	}
	return result
}
