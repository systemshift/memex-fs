package dagit

import "testing"

const (
	// Python-verified IPNS name for this DID.
	ipnsTestDID  = "did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd"
	ipnsTestName = "k51qzi5uqu5dg9ufswxt229ntzdy7p4125xzv5rtyjso89ajdujg6csfxcj260"
)

func TestDIDToIPNSName_KnownVector(t *testing.T) {
	got, err := DIDToIPNSName(ipnsTestDID)
	if err != nil {
		t.Fatalf("DIDToIPNSName: %v", err)
	}
	if got != ipnsTestName {
		t.Errorf("DIDToIPNSName mismatch\n  got:  %s\n  want: %s", got, ipnsTestName)
	}
}

func TestDIDToIPNSName_InvalidDID(t *testing.T) {
	if _, err := DIDToIPNSName("not-a-did"); err == nil {
		t.Error("expected error for invalid DID")
	}
}
