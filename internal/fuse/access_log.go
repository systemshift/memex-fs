package fuse

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/systemshift/memex-fs/internal/dag"
)

// AccessEntry is a single read-access record.
type AccessEntry struct {
	Timestamp string `json:"ts"`
	NodeID    string `json:"node"`
	Field     string `json:"field"` // "content", "meta", "type", "links"
}

// AccessLog appends read-access entries to an append-only JSONL file.
type AccessLog struct {
	path     string
	mu       sync.Mutex
	OnAccess func(nodeID string, ts time.Time) // optional callback for co-access tracking
}

// NewAccessLog creates or opens an access log at the given path.
func NewAccessLog(path string) *AccessLog {
	return &AccessLog{path: path}
}

// Log records a read access for the given node and field.
func (a *AccessLog) Log(nodeID, field string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	entry := AccessEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		NodeID:    nodeID,
		Field:     field,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("memex-fs: access log marshal: %v", err)
		return
	}

	if err := dag.SafeAppend(a.path, append(data, '\n')); err != nil {
		log.Printf("memex-fs: access log write: %v", err)
		return
	}

	if a.OnAccess != nil {
		ts, err := time.Parse(time.RFC3339Nano, entry.Timestamp)
		if err == nil {
			a.OnAccess(nodeID, ts)
		}
	}
}
