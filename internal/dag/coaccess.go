package dag

import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"
)

// CoAccessIndex tracks which nodes are read together within time-windowed sessions.
// It builds a co-occurrence matrix: if nodes A and B are accessed in the same session,
// they get their pair count incremented.
type CoAccessIndex struct {
	mu            sync.RWMutex
	pairs         map[string]map[string]int // nodeA → nodeB → count
	window        time.Duration             // session gap threshold
	currentWindow map[string]bool           // deduplicated nodes in active session
	windowStart   time.Time                 // when current session started
	lastAccess    time.Time                 // timestamp of most recent access
}

// accessLogEntry matches the JSONL format written by fuse.AccessLog.
type accessLogEntry struct {
	Timestamp string `json:"ts"`
	NodeID    string `json:"node"`
	Field     string `json:"field"`
}

// NewCoAccessIndex creates a CoAccessIndex, loading historical data from the access log.
func NewCoAccessIndex(logPath string, window time.Duration) *CoAccessIndex {
	idx := &CoAccessIndex{
		pairs:         make(map[string]map[string]int),
		window:        window,
		currentWindow: make(map[string]bool),
	}
	idx.load(logPath)
	return idx
}

// load replays the access.jsonl file into sessions.
func (idx *CoAccessIndex) load(logPath string) {
	f, err := os.Open(logPath)
	if err != nil {
		return // no log yet
	}
	defer f.Close()

	var session []string
	var sessionStart, lastTS time.Time

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var entry accessLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		ts, err := time.Parse(time.RFC3339Nano, entry.Timestamp)
		if err != nil {
			continue
		}

		if !lastTS.IsZero() && ts.Sub(lastTS) > idx.window {
			// Gap detected — flush previous session
			idx.flushSession(session)
			session = nil
			sessionStart = ts
		}
		if sessionStart.IsZero() {
			sessionStart = ts
		}

		// Deduplicate within session
		found := false
		for _, id := range session {
			if id == entry.NodeID {
				found = true
				break
			}
		}
		if !found {
			session = append(session, entry.NodeID)
		}
		lastTS = ts
	}
	// Flush final session
	idx.flushSession(session)
}

// flushSession increments co-occurrence counts for all unique pairs in the session.
func (idx *CoAccessIndex) flushSession(session []string) {
	if len(session) < 2 {
		return
	}
	for i := 0; i < len(session); i++ {
		for j := i + 1; j < len(session); j++ {
			a, b := session[i], session[j]
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

// Record is called on each FUSE read access. It manages the current session window
// and flushes when a gap is detected.
func (idx *CoAccessIndex) Record(nodeID string, ts time.Time) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.lastAccess.IsZero() && ts.Sub(idx.lastAccess) > idx.window {
		// Gap — flush current session
		session := make([]string, 0, len(idx.currentWindow))
		for id := range idx.currentWindow {
			session = append(session, id)
		}
		idx.flushSession(session)
		idx.currentWindow = make(map[string]bool)
		idx.windowStart = ts
	}
	if idx.windowStart.IsZero() {
		idx.windowStart = ts
	}

	idx.currentWindow[nodeID] = true
	idx.lastAccess = ts
}

// Related returns the top co-accessed nodes for the given node, sorted by count.
func (idx *CoAccessIndex) Related(nodeID string, limit int) []string {
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
