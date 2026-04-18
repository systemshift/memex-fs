package dag

import (
	"testing"
)

func TestEmergent_SuppressesTrivialPairs(t *testing.T) {
	repo := openTestRepo(t)

	// Two nodes that reference each other — should not emit a cluster
	// because emergentMinClusterSize = 3.
	if _, err := repo.CreateNode("a", "N", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNode("b", "N", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateLink("a", "b", "rel"); err != nil {
		t.Fatal(err)
	}

	clusters := repo.Emergent.Clusters()
	if len(clusters) != 0 {
		t.Errorf("expected no clusters for 2-node graph, got %d: %+v", len(clusters), clusters)
	}
}

func TestEmergent_TriangleFormsCluster(t *testing.T) {
	repo := openTestRepo(t)

	for _, id := range []string{"a", "b", "c"} {
		if _, err := repo.CreateNode(id, "N", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Fully-connected triangle: each node's top neighbors include the
	// other two, mutually.
	for _, pair := range [][2]string{{"a", "b"}, {"b", "c"}, {"a", "c"}} {
		if err := repo.CreateLink(pair[0], pair[1], "rel"); err != nil {
			t.Fatal(err)
		}
	}

	clusters := repo.Emergent.Clusters()
	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d: %+v", len(clusters), clusters)
	}
	if len(clusters[0].Members) != 3 {
		t.Errorf("expected 3 members, got %d: %+v", len(clusters[0].Members), clusters[0].Members)
	}
}

func TestEmergent_DisconnectedComponentsSeparate(t *testing.T) {
	repo := openTestRepo(t)

	// Two triangles with distinct types, so shared-type signal doesn't
	// bleed between them. Triangles share no links.
	for _, id := range []string{"a", "b", "c"} {
		if _, err := repo.CreateNode(id, "Academic", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"x", "y", "z"} {
		if _, err := repo.CreateNode(id, "Artist", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]string{
		{"a", "b"}, {"b", "c"}, {"a", "c"},
		{"x", "y"}, {"y", "z"}, {"x", "z"},
	} {
		if err := repo.CreateLink(pair[0], pair[1], "rel"); err != nil {
			t.Fatal(err)
		}
	}

	clusters := repo.Emergent.Clusters()
	if len(clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d: %+v", len(clusters), clusters)
	}
}

func TestEmergent_ClusterIDIsDeterministic(t *testing.T) {
	repo := openTestRepo(t)
	for _, id := range []string{"a", "b", "c"} {
		if _, err := repo.CreateNode(id, "N", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]string{{"a", "b"}, {"b", "c"}, {"a", "c"}} {
		if err := repo.CreateLink(pair[0], pair[1], "rel"); err != nil {
			t.Fatal(err)
		}
	}
	first := repo.Emergent.Clusters()
	second := repo.Emergent.Clusters()
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected exactly one cluster both runs, got %d and %d", len(first), len(second))
	}
	if first[0].ID != second[0].ID {
		t.Errorf("cluster ID not deterministic: %q vs %q", first[0].ID, second[0].ID)
	}
}

func TestEmergent_LookupByID(t *testing.T) {
	repo := openTestRepo(t)
	for _, id := range []string{"a", "b", "c"} {
		if _, err := repo.CreateNode(id, "N", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]string{{"a", "b"}, {"b", "c"}, {"a", "c"}} {
		if err := repo.CreateLink(pair[0], pair[1], "rel"); err != nil {
			t.Fatal(err)
		}
	}
	clusters := repo.Emergent.Clusters()
	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(clusters))
	}
	got := repo.Emergent.ClusterByID(clusters[0].ID)
	if got == nil {
		t.Fatal("ClusterByID returned nil for known ID")
	}
	if repo.Emergent.ClusterByID("cluster-nonexistent") != nil {
		t.Error("ClusterByID should return nil for unknown ID")
	}
}
