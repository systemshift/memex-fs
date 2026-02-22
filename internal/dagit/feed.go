package dagit

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/multiformats/go-multibase"
	"github.com/systemshift/memex-fs/internal/dag"
)

const (
	keyName        = "dagit-did"
	maxFeedEntries = 100
)

// PKCS8 DER prefix for Ed25519 private key (16 bytes).
var pkcs8Prefix = []byte{
	0x30, 0x2E, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06,
	0x03, 0x2B, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
}

// libp2p protobuf prefix: field1=varint(1) Ed25519, field2=len(32).
var libp2pPubkeyPrefix = []byte{0x08, 0x01, 0x12, 0x20}

// FeedIndex is the IPNS-published feed index.
type FeedIndex struct {
	Author string      `json:"author"`
	Posts  []FeedEntry `json:"posts"`
}

// FeedEntry is one item in a feed index.
type FeedEntry struct {
	CID       string `json:"cid"`
	Timestamp string `json:"timestamp"`
}

// FollowEntry is one followed DID.
type FollowEntry struct {
	DID          string   `json:"did"`
	Alias        string   `json:"alias,omitempty"`
	AddedAt      string   `json:"addedAt,omitempty"`
	LastSeenCids []string `json:"lastSeenCids,omitempty"`
}

// FeedManager handles publishing, following, and syncing feeds.
type FeedManager struct {
	kubo     *KuboClient
	identity *dag.Identity
	repo     *dag.Repository
	dataDir  string // .mx/dagit/
	mu       sync.Mutex
}

// NewFeedManager creates a FeedManager.
func NewFeedManager(kubo *KuboClient, identity *dag.Identity, repo *dag.Repository) *FeedManager {
	return &FeedManager{
		kubo:     kubo,
		identity: identity,
		repo:     repo,
		dataDir:  filepath.Join(repo.MxDir(), "dagit"),
	}
}

// EnsureKey imports the Ed25519 key into Kubo as "dagit-did" if not already present.
func (fm *FeedManager) EnsureKey() error {
	keys, err := fm.kubo.KeyList()
	if err != nil {
		return fmt.Errorf("list keys: %w", err)
	}
	for _, k := range keys {
		if k.Name == keyName {
			return nil
		}
	}

	seed, err := base64.StdEncoding.DecodeString(fm.identity.PrivateKey)
	if err != nil {
		return fmt.Errorf("decode seed: %w", err)
	}

	// Build PKCS8 DER: fixed 16-byte prefix + 32-byte seed
	der := append(pkcs8Prefix, seed...)
	b64 := base64.StdEncoding.EncodeToString(der)

	// Wrap in PEM (64 chars per line)
	var lines []string
	for i := 0; i < len(b64); i += 64 {
		end := i + 64
		if end > len(b64) {
			end = len(b64)
		}
		lines = append(lines, b64[i:end])
	}
	pem := "-----BEGIN PRIVATE KEY-----\n" + strings.Join(lines, "\n") + "\n-----END PRIVATE KEY-----\n"

	return fm.kubo.KeyImport(keyName, pem)
}

// DIDToIPNSName derives the IPNS name (k-prefixed base36 CIDv1) from a DID.
func DIDToIPNSName(did string) (string, error) {
	pubkey, err := dag.DecodeDIDKey(did)
	if err != nil {
		return "", err
	}

	// libp2p protobuf PublicKey: type=Ed25519(1), data=pubkey
	protobuf := append(libp2pPubkeyPrefix, pubkey...)

	// Identity multihash: 0x00 (identity hash) + varint length + data
	multihash := append([]byte{0x00, byte(len(protobuf))}, protobuf...)

	// CIDv1: 0x01 (version) + 0x72 (libp2p-key codec) + multihash
	cidBytes := append([]byte{0x01, 0x72}, multihash...)

	// Base36 with 'k' multibase prefix
	encoded, err := multibase.Encode(multibase.Base36, cidBytes)
	if err != nil {
		return "", fmt.Errorf("base36 encode: %w", err)
	}
	return encoded, nil
}

// Follow adds a DID to the following list. If alias is empty, a petname is generated.
func (fm *FeedManager) Follow(did, alias string) error {
	if !strings.HasPrefix(did, "did:key:z") {
		return fmt.Errorf("invalid DID format: %s", did)
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	entries, err := fm.loadFollowing()
	if err != nil {
		entries = []FollowEntry{}
	}

	for _, e := range entries {
		if e.DID == did {
			return fmt.Errorf("already following %s", did)
		}
	}

	if alias == "" {
		alias = PetnameFromDID(did)
	}

	entries = append(entries, FollowEntry{
		DID:          did,
		Alias:        alias,
		AddedAt:      time.Now().UTC().Format(time.RFC3339),
		LastSeenCids: []string{},
	})

	return fm.saveFollowing(entries)
}

// Unfollow removes a DID (or alias) from the following list.
func (fm *FeedManager) Unfollow(didOrAlias string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	entries, err := fm.loadFollowing()
	if err != nil {
		return fmt.Errorf("load following: %w", err)
	}

	for i, e := range entries {
		if e.DID == didOrAlias || e.Alias == didOrAlias {
			entries = append(entries[:i], entries[i+1:]...)
			return fm.saveFollowing(entries)
		}
	}
	return fmt.Errorf("not following %s", didOrAlias)
}

// ListFollowing returns the current following list.
func (fm *FeedManager) ListFollowing() ([]FollowEntry, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.loadFollowing()
}

// PublishPost creates a signed post, adds it to IPFS, updates the feed index,
// and publishes via IPNS in the background.
func (fm *FeedManager) PublishPost(content string, refs, tags []string) (string, error) {
	cid, err := Publish(fm.kubo, fm.identity, content, refs, tags)
	if err != nil {
		return "", err
	}

	// Ingest our own post as a node
	post := CreatePost(fm.identity.DID, content, refs, tags)
	post.Signature = "self" // placeholder; actual sig is on IPFS
	fm.IngestPost(post, cid)

	// Update feed index
	fm.mu.Lock()
	feed, _ := fm.loadFeedIndex()
	if feed == nil {
		feed = &FeedIndex{Author: fm.identity.DID, Posts: []FeedEntry{}}
	}
	feed.Posts = append([]FeedEntry{{
		CID:       cid,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}}, feed.Posts...)
	if len(feed.Posts) > maxFeedEntries {
		feed.Posts = feed.Posts[:maxFeedEntries]
	}
	fm.saveFeedIndex(feed)
	fm.mu.Unlock()

	// Add feed index to IPFS and publish via IPNS in background
	go func() {
		data, err := json.Marshal(feed)
		if err != nil {
			return
		}
		feedCID, err := fm.kubo.Add(data)
		if err != nil {
			log.Printf("memex-fs: feed IPFS add failed: %v", err)
			return
		}
		if err := fm.kubo.NamePublish(feedCID, keyName); err != nil {
			log.Printf("memex-fs: IPNS publish failed: %v", err)
		}
	}()

	return cid, nil
}

// CheckFeeds resolves all followed feeds, fetches new posts, verifies, and ingests them.
func (fm *FeedManager) CheckFeeds() (string, error) {
	fm.mu.Lock()
	entries, err := fm.loadFollowing()
	fm.mu.Unlock()
	if err != nil || len(entries) == 0 {
		return "Not following anyone.", nil
	}

	var lines []string
	for i := range entries {
		e := &entries[i]
		label := e.Alias
		if label == "" {
			label = e.DID[len(e.DID)-12:]
		}

		ipnsName, err := DIDToIPNSName(e.DID)
		if err != nil {
			lines = append(lines, fmt.Sprintf("%s: failed (bad DID: %v)", label, err))
			continue
		}

		feedCID, err := fm.kubo.NameResolve(ipnsName)
		if err != nil {
			lines = append(lines, fmt.Sprintf("%s: failed (%v)", label, err))
			continue
		}

		feedData, err := fm.kubo.Cat(feedCID)
		if err != nil {
			lines = append(lines, fmt.Sprintf("%s: failed (fetch: %v)", label, err))
			continue
		}

		var feed FeedIndex
		if err := json.Unmarshal(feedData, &feed); err != nil {
			lines = append(lines, fmt.Sprintf("%s: failed (parse: %v)", label, err))
			continue
		}

		if len(feed.Posts) == 0 {
			lines = append(lines, fmt.Sprintf("%s: empty feed", label))
			continue
		}

		known := make(map[string]bool)
		for _, c := range e.LastSeenCids {
			known[c] = true
		}

		ingested := 0
		for _, p := range feed.Posts {
			if known[p.CID] {
				continue
			}
			post, verified, err := Fetch(fm.kubo, p.CID)
			if err != nil || post == nil {
				continue
			}
			if post.Author != e.DID {
				continue
			}
			if !verified {
				continue
			}
			fm.IngestPost(post, p.CID)
			ingested++
		}

		// Update last seen CIDs
		var cids []string
		for _, p := range feed.Posts {
			cids = append(cids, p.CID)
		}
		e.LastSeenCids = cids

		if ingested > 0 {
			lines = append(lines, fmt.Sprintf("%s: %d new post(s)", label, ingested))
		} else {
			lines = append(lines, fmt.Sprintf("%s: up to date", label))
		}
	}

	fm.mu.Lock()
	fm.saveFollowing(entries)
	fm.mu.Unlock()

	if len(lines) == 0 {
		return "All feeds checked.", nil
	}
	return strings.Join(lines, "\n"), nil
}

// IngestPost creates a graph node from a post.
func (fm *FeedManager) IngestPost(post *Post, ipfsCID string) {
	short := ipfsCID
	if len(short) > 16 {
		short = short[:16]
	}
	nodeID := "post:" + short

	// Skip if already exists
	if fm.repo.Refs.Has(nodeID) {
		return
	}

	meta := map[string]interface{}{
		"ipfs_cid":  ipfsCID,
		"author":    post.Author,
		"timestamp": post.Timestamp,
		"verified":  true,
		"refs":      post.Refs,
		"tags":      post.Tags,
	}

	_, err := fm.repo.CreateNode(nodeID, "Post", []byte(post.Content), meta)
	if err != nil {
		log.Printf("memex-fs: ingest post %s: %v", nodeID, err)
	}
}

// DID returns the identity's DID string.
func (fm *FeedManager) DID() string {
	return fm.identity.DID
}

// PostIDsByAuthor returns post node IDs authored by a given DID.
func (fm *FeedManager) PostIDsByAuthor(did string) []string {
	// Look up all Post-type nodes and filter by author
	postIDs := fm.repo.Search.FilterByType("Post", 0)
	var result []string
	for _, id := range postIDs {
		node, err := fm.repo.GetNode(id)
		if err != nil {
			continue
		}
		if meta := node.Meta; meta != nil {
			if author, ok := meta["author"].(string); ok && author == did {
				result = append(result, id)
			}
		}
	}
	return result
}

// OwnPostIDs returns the node IDs of the user's own published posts.
func (fm *FeedManager) OwnPostIDs() []string {
	fm.mu.Lock()
	feed, _ := fm.loadFeedIndex()
	fm.mu.Unlock()

	if feed == nil {
		return nil
	}

	var ids []string
	for _, p := range feed.Posts {
		short := p.CID
		if len(short) > 16 {
			short = short[:16]
		}
		ids = append(ids, "post:"+short)
	}
	return ids
}

// --- file helpers ---

func (fm *FeedManager) followingPath() string {
	return filepath.Join(fm.dataDir, "following.json")
}

func (fm *FeedManager) feedIndexPath() string {
	return filepath.Join(fm.dataDir, "feed.json")
}

func (fm *FeedManager) loadFollowing() ([]FollowEntry, error) {
	data, err := os.ReadFile(fm.followingPath())
	if err != nil {
		if os.IsNotExist(err) {
			return []FollowEntry{}, nil
		}
		return nil, err
	}
	var entries []FollowEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return []FollowEntry{}, nil
	}
	return entries, nil
}

func (fm *FeedManager) saveFollowing(entries []FollowEntry) error {
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return dag.SafeWrite(fm.followingPath(), data, 0644)
}

func (fm *FeedManager) loadFeedIndex() (*FeedIndex, error) {
	data, err := os.ReadFile(fm.feedIndexPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var feed FeedIndex
	if err := json.Unmarshal(data, &feed); err != nil {
		return nil, err
	}
	return &feed, nil
}

func (fm *FeedManager) saveFeedIndex(feed *FeedIndex) error {
	data, err := json.MarshalIndent(feed, "", "  ")
	if err != nil {
		return err
	}
	return dag.SafeWrite(fm.feedIndexPath(), data, 0644)
}
