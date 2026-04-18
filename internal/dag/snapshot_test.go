package dag

import (
	"testing"
	"time"

	"github.com/multiformats/go-multibase"
)

func TestSnapshot_GetNodeAtCommit(t *testing.T) {
	repo := openTestRepo(t)

	// v1: alice with content "original"
	if _, err := repo.CreateNode("person:alice", "Person", []byte("original"), nil); err != nil {
		t.Fatal(err)
	}
	head, err := repo.Commits.Head()
	if err != nil || head == CidUndef {
		t.Fatalf("HEAD after create: %v / %v", head, err)
	}
	v1CID, _ := multibase.Encode(multibase.Base32, head.Bytes())

	// v2: update content
	if _, err := repo.UpdateContent("person:alice", []byte("revised")); err != nil {
		t.Fatal(err)
	}

	// Live view returns revised
	live, err := repo.GetNode("person:alice")
	if err != nil {
		t.Fatal(err)
	}
	if string(live.Content) != "revised" {
		t.Errorf("live content = %q, want revised", live.Content)
	}

	// Snapshot at v1 returns original
	commit, err := repo.Commits.Resolve(v1CID)
	if err != nil {
		t.Fatalf("Resolve v1 CID: %v", err)
	}
	snap := NewSnapshot(commit, repo.Store)
	got, err := snap.GetNode("person:alice")
	if err != nil {
		t.Fatalf("snapshot GetNode: %v", err)
	}
	if string(got.Content) != "original" {
		t.Errorf("snapshot content = %q, want original", got.Content)
	}
}

func TestSnapshot_HidesLaterCreatedNodes(t *testing.T) {
	repo := openTestRepo(t)

	if _, err := repo.CreateNode("person:alice", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}
	head, _ := repo.Commits.Head()
	v1CID, _ := multibase.Encode(multibase.Base32, head.Bytes())

	// After the snapshot point, add bob
	if _, err := repo.CreateNode("person:bob", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}

	commit, err := repo.Commits.Resolve(v1CID)
	if err != nil {
		t.Fatal(err)
	}
	snap := NewSnapshot(commit, repo.Store)

	if _, err := snap.GetNode("person:alice"); err != nil {
		t.Errorf("alice should exist at v1: %v", err)
	}
	if _, err := snap.GetNode("person:bob"); err == nil {
		t.Error("bob should NOT exist at v1 snapshot")
	}
}

func TestCommitLog_ResolveByTime(t *testing.T) {
	repo := openTestRepo(t)

	if _, err := repo.CreateNode("n:1", "N", nil, nil); err != nil {
		t.Fatal(err)
	}
	// Give a perceptible gap
	time.Sleep(20 * time.Millisecond)
	tMid := time.Now().UTC()
	time.Sleep(20 * time.Millisecond)
	if _, err := repo.CreateNode("n:2", "N", nil, nil); err != nil {
		t.Fatal(err)
	}

	commit, err := repo.Commits.Resolve(tMid.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("Resolve by time: %v", err)
	}
	// Should have resolved to the first commit (before tMid)
	snap := NewSnapshot(commit, repo.Store)
	if _, err := snap.GetNode("n:1"); err != nil {
		t.Errorf("n:1 should be in pre-tMid snapshot: %v", err)
	}
	if _, err := snap.GetNode("n:2"); err == nil {
		t.Error("n:2 should NOT be in pre-tMid snapshot")
	}
}

func TestSnapshot_LinksFromSnapshot(t *testing.T) {
	repo := openTestRepo(t)

	if _, err := repo.CreateNode("a", "N", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNode("b", "N", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateLink("a", "b", "knows"); err != nil {
		t.Fatal(err)
	}

	head, _ := repo.Commits.Head()
	cidStr, _ := multibase.Encode(multibase.Base32, head.Bytes())
	commit, err := repo.Commits.Resolve(cidStr)
	if err != nil {
		t.Fatal(err)
	}
	snap := NewSnapshot(commit, repo.Store)

	out := snap.LinksFrom("a")
	if len(out) != 1 || out[0].Target != "b" || out[0].Type != "knows" {
		t.Errorf("snapshot LinksFrom(a) = %+v, want [{a b knows}]", out)
	}
	in := snap.LinksTo("b")
	if len(in) != 1 || in[0].Source != "a" {
		t.Errorf("snapshot LinksTo(b) = %+v, want [{a b knows}]", in)
	}
}
