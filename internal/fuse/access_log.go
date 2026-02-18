package fuse

import (
	"encoding/json"
	"os"
	"sync"
	"time"
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
		return
	}

	f, err := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	f.Write(append(data, '\n'))
	f.Close()

	if a.OnAccess != nil {
		ts, err := time.Parse(time.RFC3339Nano, entry.Timestamp)
		if err == nil {
			a.OnAccess(nodeID, ts)
		}
	}
}
