package dag

import "sort"

// NeighborsIndex blends multiple relevance signals into a single ranked list
// of nodes related to a given node. It is the keystone emergent-view for the
// DAG filesystem: agents navigate from any node via /neighbors/.
//
// Signals and weights:
//   - Direct outgoing link:   10.0 per link     (authored, highest signal)
//   - Direct incoming link:   10.0 per link     (authored, highest signal)
//   - Shared link target:      3.0 per peer     (two-hop topology)
//   - Co-change:               2.0 per co-occurrence (authored — edited together)
//   - Shared type:             1.0 per peer     (weak but broad)
//   - Co-access:               0.5 per co-occurrence (usage proxy, de-weighted)
//
// Semantic similarity is intentionally absent: embeddings currently live in
// the CLI. When they move into the object store (CID-addressed) this index
// should grow a semantic signal.
type NeighborsIndex struct {
	links    *LinkIndex
	search   *SearchIndex
	coChange *CoChangeIndex
	coAccess *CoAccessIndex
	repo     nodeTypeResolver
}

// nodeTypeResolver exists to avoid an import cycle: NeighborsIndex needs a
// node's type to compute shared-type peers, but it lives in the same package
// as Repository. Repository satisfies this trivially.
type nodeTypeResolver interface {
	GetNode(id string) (*NodeEnvelope, error)
}

// NewNeighborsIndex constructs a NeighborsIndex. All signal backends must be
// non-nil; if a signal is unwanted, pass an empty one.
func NewNeighborsIndex(
	links *LinkIndex,
	search *SearchIndex,
	coChange *CoChangeIndex,
	coAccess *CoAccessIndex,
	repo nodeTypeResolver,
) *NeighborsIndex {
	return &NeighborsIndex{
		links:    links,
		search:   search,
		coChange: coChange,
		coAccess: coAccess,
		repo:     repo,
	}
}

// Weight constants so signal tuning happens in one place.
const (
	weightDirectLink  = 10.0
	weightSharedLink  = 3.0
	weightCoChange    = 2.0
	weightSharedType  = 1.0
	weightCoAccess    = 0.5
	sharedTypeCap     = 50 // avoid dominating on a type with thousands of peers
)

// Neighbors returns up to `limit` relevant neighbor IDs for nodeID, ranked by
// the blended score. The seed node itself is never included.
func (n *NeighborsIndex) Neighbors(nodeID string, limit int) []string {
	scores := make(map[string]float64)

	// 1. Direct outgoing links — heaviest signal.
	for _, l := range n.links.LinksFrom(nodeID) {
		if l.Target != nodeID {
			scores[l.Target] += weightDirectLink
		}
	}

	// 2. Direct incoming links — same weight as outgoing.
	for _, l := range n.links.LinksTo(nodeID) {
		if l.Source != nodeID {
			scores[l.Source] += weightDirectLink
		}
	}

	// 3. Shared link target (two-hop): for each node N that shares an outgoing
	// target with our seed, add a point per shared target.
	for _, l := range n.links.LinksFrom(nodeID) {
		for _, peer := range n.links.LinksTo(l.Target) {
			if peer.Source == nodeID || peer.Source == "" {
				continue
			}
			scores[peer.Source] += weightSharedLink
		}
	}

	// 4. Co-change (authored signal — same commit window).
	n.coChange.mu.RLock()
	for id, count := range n.coChange.pairs[nodeID] {
		if id == nodeID {
			continue
		}
		scores[id] += float64(count) * weightCoChange
	}
	n.coChange.mu.RUnlock()

	// 5. Shared type — broad, weak. Cap contribution so a huge type bucket
	// doesn't drown everything else.
	if node, err := n.repo.GetNode(nodeID); err == nil && node.Type != "" {
		peers := n.search.FilterByType(node.Type, sharedTypeCap+1)
		for _, p := range peers {
			if p == nodeID {
				continue
			}
			scores[p] += weightSharedType
		}
	}

	// 6. Co-access (usage proxy — de-weighted under AI automation).
	n.coAccess.mu.RLock()
	for id, count := range n.coAccess.pairs[nodeID] {
		if id == nodeID {
			continue
		}
		scores[id] += float64(count) * weightCoAccess
	}
	n.coAccess.mu.RUnlock()

	if len(scores) == 0 {
		return nil
	}

	type scored struct {
		id    string
		score float64
	}
	results := make([]scored, 0, len(scores))
	for id, score := range scores {
		results = append(results, scored{id, score})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].score != results[j].score {
			return results[i].score > results[j].score
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
