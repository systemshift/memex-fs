package dag

import (
	"encoding/json"
	"sort"
	"time"
)

// NodeEnvelope is the on-disk format for a node object.
type NodeEnvelope struct {
	V        int                    `json:"v"`
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Content  []byte                 `json:"content,omitempty"`
	Meta     map[string]interface{} `json:"meta,omitempty"`
	Created  time.Time              `json:"created"`
	Modified time.Time              `json:"modified"`
	Prev     string                 `json:"prev,omitempty"`
	Deleted  bool                   `json:"deleted,omitempty"`
}

// CanonicalJSON produces a deterministic JSON encoding with sorted keys.
func CanonicalJSON(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	// Re-decode into ordered structure and re-encode
	var raw interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	return canonicalEncode(raw)
}

func canonicalEncode(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		buf := []byte{'{'}
		for i, k := range keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			keyBytes, _ := json.Marshal(k)
			buf = append(buf, keyBytes...)
			buf = append(buf, ':')
			valBytes, err := canonicalEncode(val[k])
			if err != nil {
				return nil, err
			}
			buf = append(buf, valBytes...)
		}
		buf = append(buf, '}')
		return buf, nil

	case []interface{}:
		buf := []byte{'['}
		for i, item := range val {
			if i > 0 {
				buf = append(buf, ',')
			}
			itemBytes, err := canonicalEncode(item)
			if err != nil {
				return nil, err
			}
			buf = append(buf, itemBytes...)
		}
		buf = append(buf, ']')
		return buf, nil

	default:
		return json.Marshal(v)
	}
}
