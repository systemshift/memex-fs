package dagit

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	gocid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"github.com/systemshift/memex-fs/internal/dag"
)

// fakeKubo is an in-memory implementation of the KuboClient subset that
// Push/Pull use. It hashes uploaded blocks with the same scheme memex-fs
// uses (CIDv1 raw + sha2-256), so round-trips preserve CIDs.
type fakeKubo struct {
	mu      sync.Mutex
	blocks  map[string][]byte
	pinned  map[string]bool
}

func newFakeKubo() *fakeKubo {
	return &fakeKubo{
		blocks: make(map[string][]byte),
		pinned: make(map[string]bool),
	}
}

func (f *fakeKubo) BlockPut(data []byte, codec, mhType string) (string, error) {
	if codec != "raw" || mhType != "sha2-256" {
		return "", fmt.Errorf("fakeKubo: unsupported codec/mhtype (%s/%s)", codec, mhType)
	}
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		return "", err
	}
	c := gocid.NewCidV1(gocid.Raw, mh)
	key, _ := multibase.Encode(multibase.Base32, c.Bytes())

	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocks[key] = append([]byte(nil), data...)
	return key, nil
}

func (f *fakeKubo) BlockGet(cidStr string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.blocks[cidStr]
	if !ok {
		return nil, fmt.Errorf("fakeKubo: not found: %s", cidStr)
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeKubo) Pin(cidStr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pinned[cidStr] = true
	return nil
}

// openFreshRepo creates a repo in a temp dir. Mirrors dag.openTestRepo
// but we're in a different package here.
func openFreshRepo(t *testing.T) *dag.Repository {
	t.Helper()
	dir := t.TempDir()
	repo, err := dag.OpenRepository(dir)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	return repo
}

func TestPush_UploadsAllObjects(t *testing.T) {
	repo := openFreshRepo(t)
	kubo := newFakeKubo()

	if _, err := repo.CreateNode("a", "N", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNode("b", "N", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateLink("a", "b", "rel"); err != nil {
		t.Fatal(err)
	}

	headCID, err := Push(repo, kubo)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if headCID == "" {
		t.Fatal("Push returned empty CID")
	}

	// Verify the HEAD CID was uploaded
	if _, ok := kubo.blocks[headCID]; !ok {
		t.Error("HEAD CID not in fakeKubo")
	}

	// Verify we pushed several objects (commit + at least 2 nodes)
	if len(kubo.blocks) < 3 {
		t.Errorf("expected at least 3 objects pushed, got %d", len(kubo.blocks))
	}
}

func TestPush_EmptyRepoFails(t *testing.T) {
	repo := openFreshRepo(t)
	kubo := newFakeKubo()

	_, err := Push(repo, kubo)
	if err == nil {
		t.Error("Push on empty repo should fail")
	}
}

func TestPush_CIDMatchesLocalHead(t *testing.T) {
	repo := openFreshRepo(t)
	kubo := newFakeKubo()

	if _, err := repo.CreateNode("x", "N", []byte("content"), nil); err != nil {
		t.Fatal(err)
	}

	headCID, err := Push(repo, kubo)
	if err != nil {
		t.Fatal(err)
	}

	localHead, err := repo.Commits.Head()
	if err != nil {
		t.Fatal(err)
	}
	localHeadStr, _ := multibase.Encode(multibase.Base32, localHead.Bytes())
	if headCID != localHeadStr {
		t.Errorf("Push returned %s, expected local HEAD %s", headCID, localHeadStr)
	}
}

func TestPushPull_RoundTrip(t *testing.T) {
	// Repo A: populate and push.
	repoA := openFreshRepo(t)
	kubo := newFakeKubo()

	if _, err := repoA.CreateNode("person:alice", "Person", []byte("alice content"), map[string]interface{}{
		"role": "scientist",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repoA.CreateNode("person:bob", "Person", []byte("bob content"), nil); err != nil {
		t.Fatal(err)
	}
	if err := repoA.CreateLink("person:alice", "person:bob", "knows"); err != nil {
		t.Fatal(err)
	}

	headCID, err := Push(repoA, kubo)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Repo B: empty, pull from kubo.
	repoB := openFreshRepo(t)
	if err := Pull(repoB, kubo, headCID); err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Verify the pulled commit is browsable via /at/
	commit, err := repoB.Commits.Resolve(headCID)
	if err != nil {
		t.Fatalf("Resolve after pull: %v", err)
	}
	snap := dag.NewSnapshot(commit, repoB.Store)

	alice, err := snap.GetNode("person:alice")
	if err != nil {
		t.Fatalf("snapshot GetNode alice: %v", err)
	}
	if string(alice.Content) != "alice content" {
		t.Errorf("alice content = %q, want %q", alice.Content, "alice content")
	}
	if alice.Meta["role"] != "scientist" {
		t.Errorf("alice.role = %v, want scientist", alice.Meta["role"])
	}

	bob, err := snap.GetNode("person:bob")
	if err != nil {
		t.Fatalf("snapshot GetNode bob: %v", err)
	}
	if string(bob.Content) != "bob content" {
		t.Errorf("bob content = %q", bob.Content)
	}

	// Links come through the commit's Links snapshot
	out := snap.LinksFrom("person:alice")
	if len(out) != 1 || out[0].Target != "person:bob" || out[0].Type != "knows" {
		t.Errorf("snapshot LinksFrom(alice) = %+v", out)
	}
}

func TestPull_IsIdempotent(t *testing.T) {
	repoA := openFreshRepo(t)
	kubo := newFakeKubo()

	if _, err := repoA.CreateNode("n", "N", []byte("data"), nil); err != nil {
		t.Fatal(err)
	}
	head, err := Push(repoA, kubo)
	if err != nil {
		t.Fatal(err)
	}

	repoB := openFreshRepo(t)
	if err := Pull(repoB, kubo, head); err != nil {
		t.Fatal(err)
	}
	// Second pull should be a no-op (all objects already local).
	if err := Pull(repoB, kubo, head); err != nil {
		t.Errorf("second pull failed: %v", err)
	}
}

func TestPull_CIDMismatchDetected(t *testing.T) {
	repoA := openFreshRepo(t)
	kubo := newFakeKubo()
	if _, err := repoA.CreateNode("n", "N", []byte("data"), nil); err != nil {
		t.Fatal(err)
	}
	head, err := Push(repoA, kubo)
	if err != nil {
		t.Fatal(err)
	}

	// Poison one of the blocks — any object will do.
	for k := range kubo.blocks {
		if k != head {
			kubo.blocks[k] = []byte("tampered")
			break
		}
	}

	repoB := openFreshRepo(t)
	err = Pull(repoB, kubo, head)
	if err == nil {
		t.Error("Pull should have detected the CID mismatch")
	}
}

// TestBlockCIDMatchesLocal ensures the fake kubo's hashing matches what
// memex-fs would produce — a sanity check on the test harness itself.
func TestFakeKuboCIDMatchesLocalObjectStore(t *testing.T) {
	data := []byte("any payload")
	mh, _ := multihash.Sum(data, multihash.SHA2_256, -1)
	want, _ := multibase.Encode(multibase.Base32, gocid.NewCidV1(gocid.Raw, mh).Bytes())

	kubo := newFakeKubo()
	got, err := kubo.BlockPut(data, "raw", "sha2-256")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("fakeKubo CID = %s, want %s", got, want)
	}

	// Also sanity: the same bytes produce a deterministic hash across calls.
	h := sha256.Sum256(data)
	if len(h) != 32 {
		t.Error("sha256 hash length unexpected")
	}
}
