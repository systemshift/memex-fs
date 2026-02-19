package dagit

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/systemshift/memex-fs/internal/dag"
)

const MessageVersion = 2

// Post represents a dagit signed message.
type Post struct {
	V         int      `json:"v"`
	Type      string   `json:"type"`
	Content   string   `json:"content"`
	Author    string   `json:"author"`
	Refs      []string `json:"refs"`
	Tags      []string `json:"tags"`
	Timestamp string   `json:"timestamp"`
	Signature string   `json:"signature,omitempty"`
}

// CreatePost creates an unsigned post.
func CreatePost(did, content string, refs, tags []string) *Post {
	if refs == nil {
		refs = []string{}
	}
	if tags == nil {
		tags = []string{}
	}
	return &Post{
		V:         MessageVersion,
		Type:      "post",
		Content:   content,
		Author:    did,
		Refs:      refs,
		Tags:      tags,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// signingPayload produces the canonical JSON bytes for signing — all fields
// except "signature", with sorted keys and compact encoding. Must be
// byte-identical to Python's json.dumps(post, sort_keys=True, separators=(",",":")).
func signingPayload(post *Post) ([]byte, error) {
	// Build a map without the signature field.
	// Use []string{} explicitly for empty slices (Python outputs [] not null).
	refs := post.Refs
	if refs == nil {
		refs = []string{}
	}
	tags := post.Tags
	if tags == nil {
		tags = []string{}
	}

	m := map[string]interface{}{
		"v":         post.V,
		"type":      post.Type,
		"content":   post.Content,
		"author":    post.Author,
		"refs":      refs,
		"tags":      tags,
		"timestamp": post.Timestamp,
	}

	return dag.CanonicalJSON(m)
}

// SignPost signs a post with the given Ed25519 private key.
func SignPost(post *Post, key ed25519.PrivateKey) (*Post, error) {
	payload, err := signingPayload(post)
	if err != nil {
		return nil, fmt.Errorf("signing payload: %w", err)
	}
	sig := ed25519.Sign(key, payload)
	signed := *post
	signed.Signature = base64.StdEncoding.EncodeToString(sig)
	return &signed, nil
}

// VerifyPost verifies a post's signature against its author DID.
func VerifyPost(post *Post) (bool, error) {
	if post.Signature == "" {
		return false, nil
	}

	pubBytes, err := dag.DecodeDIDKey(post.Author)
	if err != nil {
		return false, fmt.Errorf("decode author DID: %w", err)
	}

	sig, err := base64.StdEncoding.DecodeString(post.Signature)
	if err != nil {
		return false, fmt.Errorf("decode signature: %w", err)
	}

	payload, err := signingPayload(post)
	if err != nil {
		return false, fmt.Errorf("signing payload: %w", err)
	}

	return ed25519.Verify(ed25519.PublicKey(pubBytes), payload, sig), nil
}

// Publish creates, signs, adds to IPFS, and pins a post. Returns the CID.
func Publish(kubo *KuboClient, id *dag.Identity, content string, refs, tags []string) (string, error) {
	post := CreatePost(id.DID, content, refs, tags)

	key, err := id.SigningKey()
	if err != nil {
		return "", fmt.Errorf("get signing key: %w", err)
	}

	signed, err := SignPost(post, key)
	if err != nil {
		return "", fmt.Errorf("sign post: %w", err)
	}

	data, err := json.Marshal(signed)
	if err != nil {
		return "", fmt.Errorf("marshal post: %w", err)
	}

	cid, err := kubo.Add(data)
	if err != nil {
		return "", fmt.Errorf("ipfs add: %w", err)
	}

	if err := kubo.Pin(cid); err != nil {
		return "", fmt.Errorf("ipfs pin: %w", err)
	}

	return cid, nil
}

// Fetch retrieves a post from IPFS and verifies its signature.
func Fetch(kubo *KuboClient, cid string) (*Post, bool, error) {
	data, err := kubo.Cat(cid)
	if err != nil {
		return nil, false, fmt.Errorf("ipfs cat: %w", err)
	}

	var post Post
	if err := json.Unmarshal(data, &post); err != nil {
		return nil, false, fmt.Errorf("unmarshal post: %w", err)
	}

	verified, err := VerifyPost(&post)
	if err != nil {
		return &post, false, nil
	}

	return &post, verified, nil
}
