package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// EmergentIndex surfaces graph-scale shapes that fall out of explicit
// authored structure — links, types, fields. It does NOT try to learn
// patterns from usage signals or ML; the goal is to expose clusters that
// are already implicit in the human-authored graph.
//
// Current output: mutual-top-K clusters. For each node N, we take its
// top-K neighbors (from NeighborsIndex). We form an undirected edge
// between A and B iff A is in B's top-K AND B is in A's top-K. The
// connected components of that graph are the clusters.
//
// Why mutual-top-K and not just top-K? Non-mutual inclusion drifts: a
// hub node ends up in everyone's neighbor list, but only a handful of
// those peers would reciprocate. Mutual-top-K captures nodes that have
// genuine two-way relevance.
type EmergentIndex struct {
	neighbors *NeighborsIndex
	refs      *RefStore
}

const (
	// emergentClusterTopK is how many neighbors each node considers when
	// building the mutual-top-K graph. Small numbers (3-5) favor tight
	// thematic clusters; larger numbers produce broader communities.
	emergentClusterTopK = 5

	// emergentMinClusterSize keeps trivially-small clusters out of the
	// listing. A pair is not a cluster.
	emergentMinClusterSize = 3
)

// Cluster is a set of nodes that mutually rank each other as top neighbors.
type Cluster struct {
	// ID is a deterministic short hash of the sorted member list. Changing
	// any member produces a different ID, so clusters that shift over time
	// show up as new entries rather than silently mutating.
	ID      string
	Members []string // sorted
}

// NewEmergentIndex constructs the index. It reads from refs + neighbors
// lazily — no preprocessing at startup.
func NewEmergentIndex(neighbors *NeighborsIndex, refs *RefStore) *EmergentIndex {
	return &EmergentIndex{neighbors: neighbors, refs: refs}
}

// Clusters computes the current set of mutual-top-K clusters.
// Cost is O(N * K) signal evaluations where N is node count. Acceptable
// for personal KG sizes; revisit with caching if graphs get large.
func (e *EmergentIndex) Clusters() []Cluster {
	allIDs, err := e.refs.List()
	if err != nil || len(allIDs) == 0 {
		return nil
	}

	// Build each node's top-K neighbor set.
	topK := make(map[string]map[string]struct{}, len(allIDs))
	for _, id := range allIDs {
		peers := e.neighbors.Neighbors(id, emergentClusterTopK)
		set := make(map[string]struct{}, len(peers))
		for _, p := range peers {
			set[p] = struct{}{}
		}
		topK[id] = set
	}

	// Union-find over mutual-top-K edges.
	parent := make(map[string]string, len(allIDs))
	for _, id := range allIDs {
		parent[id] = id
	}
	var find func(string) string
	find = func(x string) string {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}
	union := func(a, b string) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[ra] = rb
		}
	}
	for _, a := range allIDs {
		for b := range topK[a] {
			if peers, ok := topK[b]; ok {
				if _, mutual := peers[a]; mutual {
					union(a, b)
				}
			}
		}
	}

	// Gather components.
	groups := make(map[string][]string)
	for _, id := range allIDs {
		root := find(id)
		groups[root] = append(groups[root], id)
	}

	var clusters []Cluster
	for _, members := range groups {
		if len(members) < emergentMinClusterSize {
			continue
		}
		sort.Strings(members)
		h := sha256.Sum256([]byte(strings.Join(members, "|")))
		clusters = append(clusters, Cluster{
			ID:      "cluster-" + hex.EncodeToString(h[:4]),
			Members: members,
		})
	}

	// Sort: largest first, then by ID for determinism.
	sort.Slice(clusters, func(i, j int) bool {
		if len(clusters[i].Members) != len(clusters[j].Members) {
			return len(clusters[i].Members) > len(clusters[j].Members)
		}
		return clusters[i].ID < clusters[j].ID
	})
	return clusters
}

// ClusterByID returns the cluster with the given ID, or nil if none matches.
func (e *EmergentIndex) ClusterByID(id string) *Cluster {
	for _, c := range e.Clusters() {
		if c.ID == id {
			return &c
		}
	}
	return nil
}
