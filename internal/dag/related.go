package dag

import "sort"

// RelatednessIndex combines co-access and co-change signals into a single ranking.
type RelatednessIndex struct {
	coAccess *CoAccessIndex
	coChange *CoChangeIndex
}

// NewRelatednessIndex creates a combined relatedness index.
func NewRelatednessIndex(coAccess *CoAccessIndex, coChange *CoChangeIndex) *RelatednessIndex {
	return &RelatednessIndex{coAccess: coAccess, coChange: coChange}
}

// Related returns the top related nodes, merging co-access (weight 1.0) and
// co-change (weight 2.0) scores. Co-change is weighted higher because it
// represents intentional editing, not just observation.
func (r *RelatednessIndex) Related(nodeID string, limit int) []string {
	scores := make(map[string]float64)

	// Co-access scores (weight 1.0)
	r.coAccess.mu.RLock()
	for id, count := range r.coAccess.pairs[nodeID] {
		scores[id] += float64(count) * 1.0
	}
	r.coAccess.mu.RUnlock()

	// Co-change scores (weight 2.0)
	r.coChange.mu.RLock()
	for id, count := range r.coChange.pairs[nodeID] {
		scores[id] += float64(count) * 2.0
	}
	r.coChange.mu.RUnlock()

	if len(scores) == 0 {
		return nil
	}

	type scored struct {
		id    string
		score float64
	}
	var results []scored
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
