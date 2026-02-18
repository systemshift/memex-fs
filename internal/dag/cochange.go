package dag

import (
	"sort"
	"sync"
	"time"
)

// CoChangeIndex derives co-change signals from the commit chain.
// Nodes that changed in the same time window are considered co-changed.
type CoChangeIndex struct {
	mu      sync.RWMutex
	pairs   map[string]map[string]int // nodeA → nodeB → count
	commits *CommitLog
	window  time.Duration // temporal grouping window
}

// changeEvent is a single commit's changed refs with timestamp, used for windowing.
type changeEvent struct {
	ts      time.Time
	changed []string
}

// NewCoChangeIndex creates a CoChangeIndex from the commit log.
func NewCoChangeIndex(commits *CommitLog, window time.Duration) *CoChangeIndex {
	return &CoChangeIndex{
		pairs:   make(map[string]map[string]int),
		commits: commits,
		window:  window,
	}
}

// Build walks the commit log and groups commits into time windows.
// Within each window, it diffs consecutive commits to find changed refs,
// then increments co-change counts for all pairs of changed nodes.
func (idx *CoChangeIndex) Build() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Walk up to 1000 commits (newest first)
	commits, err := idx.commits.Log(1000)
	if err != nil || len(commits) < 2 {
		return
	}

	// Collect per-commit changed refs by diffing against parent.
	// commits are newest-first, so commits[i+1] is the parent of commits[i].
	var events []changeEvent

	for i := 0; i < len(commits)-1; i++ {
		child := commits[i]
		parent := commits[i+1]
		changed := diffRefs(parent.Refs, child.Refs)
		if len(changed) > 0 {
			events = append(events, changeEvent{ts: child.Timestamp, changed: changed})
		}
	}

	// Also handle the first commit (no parent — all refs are "new")
	if len(commits) > 0 {
		first := commits[len(commits)-1]
		if len(first.Refs) > 0 {
			changed := make([]string, 0, len(first.Refs))
			for id := range first.Refs {
				changed = append(changed, id)
			}
			events = append(events, changeEvent{ts: first.Timestamp, changed: changed})
		}
	}

	// Sort events by time (oldest first) for windowing
	sort.Slice(events, func(i, j int) bool {
		return events[i].ts.Before(events[j].ts)
	})

	// Group events into time windows and count co-changes
	var windowEvents []changeEvent
	var windowStart time.Time

	for _, evt := range events {
		if !windowStart.IsZero() && evt.ts.Sub(windowStart) > idx.window {
			idx.flushWindow(windowEvents)
			windowEvents = nil
			windowStart = evt.ts
		}
		if windowStart.IsZero() {
			windowStart = evt.ts
		}
		windowEvents = append(windowEvents, evt)
	}
	idx.flushWindow(windowEvents)
}

// flushWindow collects all unique changed nodes across events in the window,
// then increments pair counts.
func (idx *CoChangeIndex) flushWindow(events []changeEvent) {
	// Collect unique changed nodes across all events in this window
	unique := make(map[string]bool)
	for _, evt := range events {
		for _, id := range evt.changed {
			unique[id] = true
		}
	}

	nodes := make([]string, 0, len(unique))
	for id := range unique {
		nodes = append(nodes, id)
	}

	if len(nodes) < 2 {
		return
	}

	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			a, b := nodes[i], nodes[j]
			if idx.pairs[a] == nil {
				idx.pairs[a] = make(map[string]int)
			}
			if idx.pairs[b] == nil {
				idx.pairs[b] = make(map[string]int)
			}
			idx.pairs[a][b]++
			idx.pairs[b][a]++
		}
	}
}

// diffRefs compares two ref snapshots and returns the IDs that changed
// (different CID, added, or removed).
func diffRefs(parent, child map[string]string) []string {
	changed := make(map[string]bool)

	// Check for changed or added refs
	for id, cid := range child {
		parentCID, exists := parent[id]
		if !exists || parentCID != cid {
			changed[id] = true
		}
	}

	// Check for removed refs
	for id := range parent {
		if _, exists := child[id]; !exists {
			changed[id] = true
		}
	}

	result := make([]string, 0, len(changed))
	for id := range changed {
		result = append(result, id)
	}
	return result
}

// Related returns the top co-changed nodes for the given node, sorted by count.
func (idx *CoChangeIndex) Related(nodeID string, limit int) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	peers := idx.pairs[nodeID]
	if len(peers) == 0 {
		return nil
	}

	type scored struct {
		id    string
		count int
	}
	var results []scored
	for id, count := range peers {
		results = append(results, scored{id, count})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].count != results[j].count {
			return results[i].count > results[j].count
		}
		return results[i].id < results[j].id
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.id
	}
	return ids
}
