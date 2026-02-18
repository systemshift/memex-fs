package dag

import "time"

// CommitObject is a Merkle DAG commit — a snapshot of all refs and links at a point in time.
// Serialized via CanonicalJSON and stored in the ObjectStore like any other object.
type CommitObject struct {
	V         int               `json:"v"`
	Parent    string            `json:"parent,omitempty"` // CID (base32) of previous commit
	Timestamp time.Time         `json:"timestamp"`
	Refs      map[string]string `json:"refs"`  // id → CID (base32)
	Links     []LinkEntry       `json:"links"` // sorted snapshot of all links
	Message   string            `json:"message,omitempty"`
}
