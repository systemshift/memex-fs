package dag

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
)

const identityRelPath = ".config/memex/identity.json"

// base58btc alphabet (Bitcoin)
const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// ed25519Multicodec is the multicodec prefix for Ed25519 public keys (0xED01).
var ed25519Multicodec = []byte{0xed, 0x01}

// Identity holds an Ed25519 keypair and the derived DID.
type Identity struct {
	DID        string `json:"did"`
	PublicKey  string `json:"public_key"`  // base64-encoded 32 bytes
	PrivateKey string `json:"private_key"` // base64-encoded 32-byte seed
}

// identityPath returns ~/.config/memex/identity.json.
func identityPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, identityRelPath)
}

// LoadIdentity reads the shared identity file, or generates a new one if missing.
func LoadIdentity() (*Identity, error) {
	path := identityPath()
	if path == "" {
		return nil, fmt.Errorf("cannot determine home directory")
	}

	data, err := os.ReadFile(path)
	if err == nil {
		var id Identity
		if err := json.Unmarshal(data, &id); err != nil {
			return nil, fmt.Errorf("parse identity: %w", err)
		}
		return &id, nil
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read identity: %w", err)
	}

	// Generate new identity
	return generateIdentity(path)
}

// generateIdentity creates a new Ed25519 keypair and writes it to disk.
func generateIdentity(path string) (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	// ed25519.PrivateKey is 64 bytes (seed+public), we store just the 32-byte seed
	seed := priv.Seed()

	did := encodeDIDKey([]byte(pub))

	id := &Identity{
		DID:        did,
		PublicKey:  base64.StdEncoding.EncodeToString(pub),
		PrivateKey: base64.StdEncoding.EncodeToString(seed),
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create identity dir: %w", err)
	}

	data, err := json.MarshalIndent(id, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal identity: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return nil, fmt.Errorf("write identity: %w", err)
	}

	fmt.Printf("memex-fs: generated new identity %s\n", did)
	fmt.Printf("memex-fs: stored at %s\n", path)
	return id, nil
}

// encodeDIDKey encodes a raw Ed25519 public key as did:key:z... using
// multicodec 0xED01 prefix and base58btc encoding.
func encodeDIDKey(publicKey []byte) string {
	prefixed := append(ed25519Multicodec, publicKey...)

	// Base58btc encode
	num := new(big.Int).SetBytes(prefixed)
	zero := big.NewInt(0)
	base := big.NewInt(58)
	mod := new(big.Int)

	var encoded []byte
	for num.Cmp(zero) > 0 {
		num.DivMod(num, base, mod)
		encoded = append([]byte{base58Alphabet[mod.Int64()]}, encoded...)
	}

	// Handle leading zero bytes
	for _, b := range prefixed {
		if b == 0 {
			encoded = append([]byte{'1'}, encoded...)
		} else {
			break
		}
	}

	return "did:key:z" + string(encoded)
}
