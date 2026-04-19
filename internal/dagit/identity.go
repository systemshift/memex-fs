package dagit

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/multiformats/go-multibase"
	"github.com/systemshift/memex-fs/internal/dag"
)

// HeadKeyName is the Kubo keystore name for publishing commit HEAD CIDs
// over IPNS. It's distinct from any future social/publishing key so the
// two namespaces never collide.
const HeadKeyName = "memex-head"

// PKCS8 DER prefix for Ed25519 private key (16 bytes).
// Used when importing the repo identity into the Kubo keystore.
var pkcs8Prefix = []byte{
	0x30, 0x2E, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06,
	0x03, 0x2B, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
}

// libp2p protobuf prefix: field1=varint(1) Ed25519, field2=len(32).
// Used when deriving IPNS names from Ed25519 DIDs.
var libp2pPubkeyPrefix = []byte{0x08, 0x01, 0x12, 0x20}

// DIDToIPNSName derives the IPNS name (k-prefixed base36 CIDv1 over a
// libp2p public-key multihash) from an ed25519-encoded did:key. The
// returned string is what Kubo's /name/resolve expects.
//
// This is needed so pull can accept a DID directly and resolve to the
// latest published commit CID.
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

	// Base36 with 'k' multibase prefix.
	encoded, err := multibase.Encode(multibase.Base36, cidBytes)
	if err != nil {
		return "", fmt.Errorf("base36 encode: %w", err)
	}
	return encoded, nil
}

// EnsureKey imports the repo's Ed25519 identity into the Kubo keystore
// under keyName if not already present. Idempotent: a second call is a
// no-op. Returns an error if Kubo rejects the import.
//
// This is the one-time setup step before NamePublish can be used.
func EnsureKey(kubo *KuboClient, identity *dag.Identity, keyName string) error {
	keys, err := kubo.KeyList()
	if err != nil {
		return fmt.Errorf("list keys: %w", err)
	}
	for _, k := range keys {
		if k.Name == keyName {
			return nil
		}
	}

	seed, err := base64.StdEncoding.DecodeString(identity.PrivateKey)
	if err != nil {
		return fmt.Errorf("decode seed: %w", err)
	}

	// PKCS8 DER: fixed 16-byte prefix + 32-byte seed.
	der := append(pkcs8Prefix, seed...)
	b64 := base64.StdEncoding.EncodeToString(der)

	// Wrap in PEM (64 chars per line).
	var lines []string
	for i := 0; i < len(b64); i += 64 {
		end := i + 64
		if end > len(b64) {
			end = len(b64)
		}
		lines = append(lines, b64[i:end])
	}
	pem := "-----BEGIN PRIVATE KEY-----\n" + strings.Join(lines, "\n") + "\n-----END PRIVATE KEY-----\n"

	return kubo.KeyImport(keyName, pem)
}
