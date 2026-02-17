package dag

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// SearchIndex is an in-memory inverted index for full-text search.
type SearchIndex struct {
	mu    sync.RWMutex
	index map[string]map[string]bool // term -> set of ref IDs
	types map[string]map[string]bool // type -> set of ref IDs
}

// NewSearchIndex creates an empty SearchIndex.
func NewSearchIndex() *SearchIndex {
	return &SearchIndex{
		index: make(map[string]map[string]bool),
		types: make(map[string]map[string]bool),
	}
}

// tokenize splits text into lowercase terms.
func tokenize(text string) []string {
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	// Deduplicate
	seen := make(map[string]bool)
	var result []string
	for _, w := range words {
		if len(w) < 2 {
			continue
		}
		if !seen[w] {
			seen[w] = true
			result = append(result, w)
		}
	}
	return result
}

// IndexNode adds a node to the search and type indexes.
func (s *SearchIndex) IndexNode(id string, node *NodeEnvelope) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Build searchable text from id + type + content + meta values
	var parts []string
	parts = append(parts, id, node.Type)

	// Decode content if it's text-like
	if node.Content != nil {
		// Content is stored as []byte, try to use it as text
		text := string(node.Content)
		// Check if it's base64 (from JSON unmarshal of []byte)
		if decoded, err := base64.StdEncoding.DecodeString(text); err == nil {
			parts = append(parts, string(decoded))
		} else {
			parts = append(parts, text)
		}
	}

	// Index meta values
	for _, v := range node.Meta {
		parts = append(parts, fmt.Sprintf("%v", v))
	}

	// Tokenize and index
	terms := tokenize(strings.Join(parts, " "))
	for _, term := range terms {
		if s.index[term] == nil {
			s.index[term] = make(map[string]bool)
		}
		s.index[term][id] = true
	}

	// Type index
	if node.Type != "" {
		typ := node.Type
		if s.types[typ] == nil {
			s.types[typ] = make(map[string]bool)
		}
		s.types[typ][id] = true
	}
}

// RemoveNode removes a node from the search and type indexes.
func (s *SearchIndex) RemoveNode(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for term, ids := range s.index {
		delete(ids, id)
		if len(ids) == 0 {
			delete(s.index, term)
		}
	}
	for typ, ids := range s.types {
		delete(ids, id)
		if len(ids) == 0 {
			delete(s.types, typ)
		}
	}
}

// Search queries the inverted index and returns ref IDs ranked by term match count.
func (s *SearchIndex) Search(query string, limit int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	terms := tokenize(query)
	if len(terms) == 0 {
		return nil
	}

	scores := make(map[string]int)
	for _, term := range terms {
		for id := range s.index[term] {
			scores[id]++
		}
	}

	type scored struct {
		id    string
		score int
	}
	var results []scored
	for id, score := range scores {
		results = append(results, scored{id, score})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
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

// AllTypes returns a sorted list of all known type strings.
func (s *SearchIndex) AllTypes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	types := make([]string, 0, len(s.types))
	for t := range s.types {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// FilterByType returns all ref IDs with the given type.
func (s *SearchIndex) FilterByType(typ string, limit int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.types[typ]))
	for id := range s.types[typ] {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}
