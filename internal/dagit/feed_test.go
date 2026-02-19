package dagit

import (
	"testing"

	"github.com/systemshift/memex-fs/internal/dag"
)

const (
	feedTestDID  = "did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd"
	feedTestDID2 = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"

	// Python-verified IPNS name for feedTestDID
	feedTestIPNS = "k51qzi5uqu5dg9ufswxt229ntzdy7p4125xzv5rtyjso89ajdujg6csfxcj260"
)

func openTestFeedManager(t *testing.T) *FeedManager {
	t.Helper()
	dir := t.TempDir()
	repo, err := dag.OpenRepository(dir)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	id := &dag.Identity{
		DID:        feedTestDID,
		PublicKey:  "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg=",
		PrivateKey: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
	}
	return NewFeedManager(nil, id, repo)
}

func TestDIDToIPNSName_KnownVector(t *testing.T) {
	got, err := DIDToIPNSName(feedTestDID)
	if err != nil {
		t.Fatalf("DIDToIPNSName: %v", err)
	}
	if got != feedTestIPNS {
		t.Errorf("DIDToIPNSName mismatch\n  got:  %s\n  want: %s", got, feedTestIPNS)
	}
}

func TestDIDToIPNSName_InvalidDID(t *testing.T) {
	_, err := DIDToIPNSName("not-a-did")
	if err == nil {
		t.Error("expected error for invalid DID")
	}
}

func TestFollow_ListFollowing_Unfollow(t *testing.T) {
	fm := openTestFeedManager(t)

	if err := fm.Follow(feedTestDID2, ""); err != nil {
		t.Fatalf("Follow: %v", err)
	}

	entries, err := fm.ListFollowing()
	if err != nil {
		t.Fatalf("ListFollowing: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].DID != feedTestDID2 {
		t.Errorf("DID = %q, want %q", entries[0].DID, feedTestDID2)
	}
	// Should have auto-generated petname
	if entries[0].Alias == "" {
		t.Error("expected auto-generated alias")
	}
	if entries[0].Alias != PetnameFromDID(feedTestDID2) {
		t.Errorf("alias = %q, want %q", entries[0].Alias, PetnameFromDID(feedTestDID2))
	}

	if err := fm.Unfollow(feedTestDID2); err != nil {
		t.Fatalf("Unfollow: %v", err)
	}

	entries, err = fm.ListFollowing()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after unfollow, got %d", len(entries))
	}
}

func TestFollow_Duplicate(t *testing.T) {
	fm := openTestFeedManager(t)

	if err := fm.Follow(feedTestDID2, ""); err != nil {
		t.Fatal(err)
	}
	err := fm.Follow(feedTestDID2, "")
	if err == nil {
		t.Error("expected error following same DID twice")
	}
}

func TestFollow_InvalidDID(t *testing.T) {
	fm := openTestFeedManager(t)

	err := fm.Follow("not-a-did", "")
	if err == nil {
		t.Error("expected error for invalid DID format")
	}
}

func TestUnfollow_ByAlias(t *testing.T) {
	fm := openTestFeedManager(t)

	fm.Follow(feedTestDID2, "my-friend")

	if err := fm.Unfollow("my-friend"); err != nil {
		t.Fatalf("Unfollow by alias: %v", err)
	}

	entries, _ := fm.ListFollowing()
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestUnfollow_NotFollowing(t *testing.T) {
	fm := openTestFeedManager(t)

	err := fm.Unfollow(feedTestDID2)
	if err == nil {
		t.Error("expected error unfollowing unknown DID")
	}
}

func TestIngestPost(t *testing.T) {
	fm := openTestFeedManager(t)

	post := &Post{
		V:         2,
		Type:      "post",
		Content:   "ingested content",
		Author:    feedTestDID,
		Refs:      []string{},
		Tags:      []string{"test"},
		Timestamp: "2024-06-01T12:00:00Z",
		Signature: "fakesig",
	}

	fm.IngestPost(post, "QmTestCID12345678")
	nodeID := "post:QmTestCID1234567"

	node, err := fm.repo.GetNode(nodeID)
	if err != nil {
		t.Fatalf("GetNode after ingest: %v", err)
	}
	if node.Type != "Post" {
		t.Errorf("Type = %q, want %q", node.Type, "Post")
	}
	if string(node.Content) != "ingested content" {
		t.Errorf("Content = %q, want %q", node.Content, "ingested content")
	}
	if node.Meta["author"] != feedTestDID {
		t.Errorf("Meta[author] = %v, want %q", node.Meta["author"], feedTestDID)
	}
	if node.Meta["ipfs_cid"] != "QmTestCID12345678" {
		t.Errorf("Meta[ipfs_cid] = %v, want %q", node.Meta["ipfs_cid"], "QmTestCID12345678")
	}
}

func TestIngestPost_Dedup(t *testing.T) {
	fm := openTestFeedManager(t)

	post := &Post{
		V:         2,
		Type:      "post",
		Content:   "dupe test",
		Author:    feedTestDID,
		Refs:      []string{},
		Tags:      []string{},
		Timestamp: "2024-06-01T12:00:00Z",
	}

	fm.IngestPost(post, "QmDupe12345678901")
	fm.IngestPost(post, "QmDupe12345678901") // second time — should be a no-op

	// Verify node exists (CID truncated to 16 chars)
	nodeID := "post:QmDupe1234567890"
	_, err := fm.repo.GetNode(nodeID)
	if err != nil {
		t.Fatalf("node should exist: %v", err)
	}

	// Verify only one Post node
	postIDs := fm.repo.Search.FilterByType("Post", 0)
	if len(postIDs) != 1 {
		t.Errorf("expected 1 Post node, got %d: %v", len(postIDs), postIDs)
	}
}

func TestFeedManager_DID(t *testing.T) {
	fm := openTestFeedManager(t)
	if fm.DID() != feedTestDID {
		t.Errorf("DID() = %q, want %q", fm.DID(), feedTestDID)
	}
}

func TestFollow_CustomAlias(t *testing.T) {
	fm := openTestFeedManager(t)

	if err := fm.Follow(feedTestDID2, "alice"); err != nil {
		t.Fatal(err)
	}

	entries, _ := fm.ListFollowing()
	if len(entries) != 1 {
		t.Fatal("expected 1 entry")
	}
	if entries[0].Alias != "alice" {
		t.Errorf("alias = %q, want %q", entries[0].Alias, "alice")
	}
}
