package dag

import (
	"testing"
)

func TestNeighbors_DirectLinksDominate(t *testing.T) {
	repo := openTestRepo(t)

	// Seed: alice, bob (directly linked to alice), carol (shared type Person only).
	if _, err := repo.CreateNode("person:alice", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNode("person:bob", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNode("person:carol", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateLink("person:alice", "person:bob", "knows"); err != nil {
		t.Fatal(err)
	}

	got := repo.Neighbors.Neighbors("person:alice", 10)
	if len(got) == 0 {
		t.Fatal("want at least one neighbor, got none")
	}
	if got[0] != "person:bob" {
		t.Errorf("top neighbor = %q, want %q (direct link should outrank shared-type-only peer)", got[0], "person:bob")
	}
}

func TestNeighbors_ExcludesSelf(t *testing.T) {
	repo := openTestRepo(t)
	if _, err := repo.CreateNode("person:alice", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNode("person:bob", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateLink("person:alice", "person:bob", "knows"); err != nil {
		t.Fatal(err)
	}
	got := repo.Neighbors.Neighbors("person:alice", 10)
	for _, id := range got {
		if id == "person:alice" {
			t.Errorf("neighbors returned self: %v", got)
		}
	}
}

func TestNeighbors_SharedLinkTarget(t *testing.T) {
	repo := openTestRepo(t)

	// alice -> acme, bob -> acme. bob should surface as neighbor of alice
	// via shared outgoing target (two-hop).
	for _, id := range []string{"person:alice", "person:bob", "org:acme"} {
		typ := "Person"
		if id == "org:acme" {
			typ = "Org"
		}
		if _, err := repo.CreateNode(id, typ, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.CreateLink("person:alice", "org:acme", "works_at"); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateLink("person:bob", "org:acme", "works_at"); err != nil {
		t.Fatal(err)
	}

	got := repo.Neighbors.Neighbors("person:alice", 10)
	seenBob := false
	for _, id := range got {
		if id == "person:bob" {
			seenBob = true
		}
	}
	if !seenBob {
		t.Errorf("bob should appear via shared outgoing target; neighbors = %v", got)
	}
}

func TestNeighbors_IncomingLinksCount(t *testing.T) {
	repo := openTestRepo(t)

	if _, err := repo.CreateNode("paper:1", "Paper", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNode("person:alice", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateLink("person:alice", "paper:1", "cites"); err != nil {
		t.Fatal(err)
	}

	got := repo.Neighbors.Neighbors("paper:1", 10)
	if len(got) == 0 || got[0] != "person:alice" {
		t.Errorf("incoming link source should surface as top neighbor; got %v", got)
	}
}

func TestNeighbors_EmptyWhenIsolated(t *testing.T) {
	repo := openTestRepo(t)
	if _, err := repo.CreateNode("loner:1", "Loner", nil, nil); err != nil {
		t.Fatal(err)
	}
	got := repo.Neighbors.Neighbors("loner:1", 10)
	if len(got) != 0 {
		t.Errorf("isolated node with unique type should have no neighbors; got %v", got)
	}
}
