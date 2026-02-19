package dagit

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"

	"github.com/systemshift/memex-fs/internal/dag"
)

// Test vectors generated from Python nacl with seed=bytes(range(32)).
const (
	testSeedB64   = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	testPubkeyB64 = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="
	testDID       = "did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd"

	// Exact Python output of json.dumps(post, sort_keys=True, separators=(",",":"))
	testPayload = `{"author":"did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd","content":"hello from test","refs":[],"tags":[],"timestamp":"2024-01-01T00:00:00Z","type":"post","v":2}`

	// Signature from Python: sk.sign(payload.encode()).signature
	testSignatureB64 = "kxvUxysm1oFI77Nm49d2xb3qXRGXRLzd2jEAPPjdDWi51BmoVCwBk6fxmI0e4KmRHzEr43QWN0EUL5OkQaw6DA=="
)

func testIdentity(t *testing.T) *dag.Identity {
	t.Helper()
	return &dag.Identity{
		DID:        testDID,
		PublicKey:  testPubkeyB64,
		PrivateKey: testSeedB64,
	}
}

func testPost() *Post {
	return &Post{
		V:         2,
		Type:      "post",
		Content:   "hello from test",
		Author:    testDID,
		Refs:      []string{},
		Tags:      []string{},
		Timestamp: "2024-01-01T00:00:00Z",
	}
}

func TestSigningPayload_PythonEquivalence(t *testing.T) {
	post := testPost()
	got, err := signingPayload(post)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != testPayload {
		t.Errorf("payload mismatch\n  got:  %s\n  want: %s", got, testPayload)
	}
}

func TestSigningPayload_EmptySlices(t *testing.T) {
	post := &Post{
		V:         2,
		Type:      "post",
		Content:   "x",
		Author:    "did:key:zTest",
		Refs:      nil,
		Tags:      nil,
		Timestamp: "2024-01-01T00:00:00Z",
	}
	got, err := signingPayload(post)
	if err != nil {
		t.Fatal(err)
	}
	s := string(got)
	// Must contain "refs":[] and "tags":[], never null
	if contains(s, `"refs":null`) || contains(s, `"tags":null`) {
		t.Errorf("nil slices should become [], got: %s", s)
	}
	if !contains(s, `"refs":[]`) || !contains(s, `"tags":[]`) {
		t.Errorf("expected empty arrays, got: %s", s)
	}
}

func TestSignPost_VerifyPost_RoundTrip(t *testing.T) {
	id := testIdentity(t)
	priv, _ := id.SigningKey()

	post := testPost()
	signed, err := SignPost(post, priv)
	if err != nil {
		t.Fatalf("SignPost: %v", err)
	}
	if signed.Signature == "" {
		t.Fatal("signature is empty")
	}

	ok, err := VerifyPost(signed)
	if err != nil {
		t.Fatalf("VerifyPost: %v", err)
	}
	if !ok {
		t.Error("VerifyPost returned false for valid signature")
	}
}

func TestVerifyPost_PythonSignature(t *testing.T) {
	post := testPost()
	post.Signature = testSignatureB64

	ok, err := VerifyPost(post)
	if err != nil {
		t.Fatalf("VerifyPost: %v", err)
	}
	if !ok {
		t.Error("Go rejected a Python-signed post — interop broken")
	}
}

func TestVerifyPost_MissingSignature(t *testing.T) {
	post := testPost()
	post.Signature = ""

	ok, err := VerifyPost(post)
	if err != nil {
		t.Fatalf("VerifyPost: %v", err)
	}
	if ok {
		t.Error("expected false for missing signature")
	}
}

func TestVerifyPost_WrongAuthor(t *testing.T) {
	id := testIdentity(t)
	priv, _ := id.SigningKey()

	post := testPost()
	signed, _ := SignPost(post, priv)

	// Tamper with the author
	signed.Author = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
	ok, err := VerifyPost(signed)
	if err != nil {
		// Error from decoding a different valid DID is fine
		t.Logf("VerifyPost error (expected): %v", err)
	}
	if ok {
		t.Error("expected verification to fail with wrong author")
	}
}

func TestVerifyPost_TamperedContent(t *testing.T) {
	id := testIdentity(t)
	priv, _ := id.SigningKey()

	post := testPost()
	signed, _ := SignPost(post, priv)

	// Tamper with the content
	signed.Content = "tampered content"
	ok, err := VerifyPost(signed)
	if err != nil {
		t.Fatalf("VerifyPost: %v", err)
	}
	if ok {
		t.Error("expected verification to fail with tampered content")
	}
}

func TestSignPost_GoSignature_VerifiableByPython(t *testing.T) {
	// Sign with Go using the known seed, then verify the payload+signature
	// would also be accepted by Python.
	seed, _ := base64.StdEncoding.DecodeString(testSeedB64)
	priv := ed25519.NewKeyFromSeed(seed)

	post := testPost()
	signed, err := SignPost(post, priv)
	if err != nil {
		t.Fatal(err)
	}

	// Decode the signature and verify manually
	sig, _ := base64.StdEncoding.DecodeString(signed.Signature)
	payload, _ := signingPayload(post)

	pub, _ := base64.StdEncoding.DecodeString(testPubkeyB64)
	if !ed25519.Verify(ed25519.PublicKey(pub), payload, sig) {
		t.Error("manual verification failed")
	}

	// The payload should match Python's output exactly
	if string(payload) != testPayload {
		t.Errorf("payload drift from Python vector\n  got:  %s\n  want: %s", payload, testPayload)
	}
}

func TestCreatePost_DefaultSlices(t *testing.T) {
	post := CreatePost("did:key:zTest", "hello", nil, nil)
	if post.Refs == nil {
		t.Error("Refs should not be nil")
	}
	if post.Tags == nil {
		t.Error("Tags should not be nil")
	}
	if len(post.Refs) != 0 {
		t.Errorf("Refs should be empty, got %v", post.Refs)
	}
	if len(post.Tags) != 0 {
		t.Errorf("Tags should be empty, got %v", post.Tags)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && searchString(s, sub)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
