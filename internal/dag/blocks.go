package dag

import (
	"regexp"
	"strings"
)

// Blocks splits a node's content into paragraph-level blocks. A block is any
// run of non-blank lines separated from neighbors by one or more blank lines.
// This matches the ROAM / Markdown convention: paragraphs, list items that
// are separated by a blank line, and fenced blocks all become addressable units.
//
// Block indices are 1-based, intentionally. FUSE presents them as `b0001`,
// `b0002`, ... so a shell-sorted listing matches the source order. Indices
// are stable across edits as long as paragraph ordering doesn't change;
// re-ordering a document renumbers its blocks.
type Block struct {
	Index int    // 1-based position within the parent node
	Text  string // block contents, trimmed
}

// blankLineSplit matches one or more blank lines (possibly with whitespace).
// It's the delimiter between blocks.
var blankLineSplit = regexp.MustCompile(`(?m)^\s*\n`)

// Blocks returns the addressable blocks for the given content. Empty content
// yields no blocks; a single paragraph yields one.
func Blocks(content []byte) []Block {
	if len(content) == 0 {
		return nil
	}
	// Normalize line endings so the regex matches regardless of platform.
	s := strings.ReplaceAll(string(content), "\r\n", "\n")
	// Split on runs of blank lines. Multiple consecutive blank lines produce
	// empty strings which we filter out.
	raw := blankLineSplit.Split(s, -1)

	blocks := make([]Block, 0, len(raw))
	idx := 1
	for _, part := range raw {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		blocks = append(blocks, Block{Index: idx, Text: trimmed})
		idx++
	}
	return blocks
}
