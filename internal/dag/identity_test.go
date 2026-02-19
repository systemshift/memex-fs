package dag

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"
)

// Test vectors generated from Python nacl with deterministic seed bytes(range(32)).
const (
	testSeedB64   = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	testPubkeyB64 = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="
	testDID       = "did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd"
)

func testIdentity(t *testing.T) *Identity {
	t.Helper()
	return &Identity{
		DID:        testDID,
		PublicKey:  testPubkeyB64,
		PrivateKey: testSeedB64,
	}
}

func TestEncodeDIDKey_KnownVector(t *testing.T) {
	pub, err := base64.StdEncoding.DecodeString(testPubkeyB64)
	if err != nil {
		t.Fatal(err)
	}
	got := encodeDIDKey(pub)
	if got != testDID {
		t.Errorf("encodeDIDKey mismatch\n  got:  %s\n  want: %s", got, testDID)
	}
}

func TestDecodeDIDKey_KnownVector(t *testing.T) {
	pub, err := DecodeDIDKey(testDID)
	if err != nil {
		t.Fatalf("DecodeDIDKey: %v", err)
	}
	wantPub, _ := base64.StdEncoding.DecodeString(testPubkeyB64)
	if len(pub) != len(wantPub) {
		t.Fatalf("pubkey length %d, want %d", len(pub), len(wantPub))
	}
	for i := range pub {
		if pub[i] != wantPub[i] {
			t.Fatalf("pubkey byte %d: got %02x, want %02x", i, pub[i], wantPub[i])
		}
	}
}

func TestDID_RoundTrip(t *testing.T) {
	// Generate a fresh key, encode to DID, decode back, compare.
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	did := encodeDIDKey([]byte(pub))
	decoded, err := DecodeDIDKey(did)
	if err != nil {
		t.Fatalf("DecodeDIDKey round-trip: %v", err)
	}
	if len(decoded) != ed25519.PublicKeySize {
		t.Fatalf("decoded key length %d, want %d", len(decoded), ed25519.PublicKeySize)
	}
	for i := range decoded {
		if decoded[i] != pub[i] {
			t.Fatalf("byte %d mismatch: got %02x want %02x", i, decoded[i], pub[i])
		}
	}
}

func TestDecodeDIDKey_InvalidPrefix(t *testing.T) {
	_, err := DecodeDIDKey("bad:key:z123")
	if err == nil {
		t.Error("expected error for invalid prefix")
	}
}

func TestDecodeDIDKey_ShortInput(t *testing.T) {
	_, err := DecodeDIDKey("did:key:z")
	if err == nil {
		t.Error("expected error for empty base58 payload")
	}
}

func TestDecodeDIDKey_BadBase58(t *testing.T) {
	// '0', 'O', 'I', 'l' are not in base58btc alphabet
	_, err := DecodeDIDKey("did:key:z0OIl")
	if err == nil {
		t.Error("expected error for invalid base58 characters")
	}
}

func TestSigningKey_VerifyKey_RoundTrip(t *testing.T) {
	id := testIdentity(t)

	priv, err := id.SigningKey()
	if err != nil {
		t.Fatalf("SigningKey: %v", err)
	}
	pub, err := id.VerifyKey()
	if err != nil {
		t.Fatalf("VerifyKey: %v", err)
	}

	msg := []byte("test message")
	sig := ed25519.Sign(priv, msg)
	if !ed25519.Verify(pub, msg, sig) {
		t.Error("signature verification failed")
	}
}

func TestSigningKey_DerivesPubkey(t *testing.T) {
	id := testIdentity(t)

	priv, err := id.SigningKey()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := id.VerifyKey()
	if err != nil {
		t.Fatal(err)
	}

	// The public key derived from the private key should match.
	derivedPub := priv.Public().(ed25519.PublicKey)
	if len(derivedPub) != len(pub) {
		t.Fatalf("pubkey lengths differ: %d vs %d", len(derivedPub), len(pub))
	}
	for i := range derivedPub {
		if derivedPub[i] != pub[i] {
			t.Fatalf("pubkey byte %d: got %02x want %02x", i, derivedPub[i], pub[i])
		}
	}
}
