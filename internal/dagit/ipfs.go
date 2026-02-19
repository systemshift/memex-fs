package dagit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// KuboClient is an HTTP client for the Kubo (IPFS) daemon API.
type KuboClient struct {
	apiURL string
	client *http.Client
}

// KeyInfo represents a key in the Kubo keystore.
type KeyInfo struct {
	Name string `json:"Name"`
	ID   string `json:"Id"`
}

// NewKuboClient creates a client for the Kubo API at the given URL.
func NewKuboClient(apiURL string) *KuboClient {
	return &KuboClient{
		apiURL: strings.TrimRight(apiURL, "/"),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// IsAvailable checks if the Kubo daemon is reachable.
func (k *KuboClient) IsAvailable() bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Post(k.apiURL+"/id", "", nil)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

// Add uploads content to IPFS and returns the CID.
func (k *KuboClient) Add(content []byte) (string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", "data")
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(content); err != nil {
		return "", fmt.Errorf("write form data: %w", err)
	}
	w.Close()

	resp, err := k.client.Post(k.apiURL+"/add", w.FormDataContentType(), &buf)
	if err != nil {
		return "", fmt.Errorf("ipfs add: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ipfs add: status %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Hash string `json:"Hash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("ipfs add: parse response: %w", err)
	}
	return result.Hash, nil
}

// Cat retrieves content from IPFS by CID.
func (k *KuboClient) Cat(cid string) ([]byte, error) {
	resp, err := k.client.Post(k.apiURL+"/cat?arg="+cid, "", nil)
	if err != nil {
		return nil, fmt.Errorf("ipfs cat: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ipfs cat: status %d: %s", resp.StatusCode, body)
	}

	return io.ReadAll(resp.Body)
}

// Pin pins content to prevent garbage collection.
func (k *KuboClient) Pin(cid string) error {
	resp, err := k.client.Post(k.apiURL+"/pin/add?arg="+cid, "", nil)
	if err != nil {
		return fmt.Errorf("ipfs pin: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("ipfs pin: status %d", resp.StatusCode)
	}
	return nil
}

// KeyList lists all keys in the Kubo keystore.
func (k *KuboClient) KeyList() ([]KeyInfo, error) {
	resp, err := k.client.Post(k.apiURL+"/key/list", "", nil)
	if err != nil {
		return nil, fmt.Errorf("ipfs key/list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ipfs key/list: status %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Keys []KeyInfo `json:"Keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("ipfs key/list: parse: %w", err)
	}
	return result.Keys, nil
}

// KeyImport imports a PEM-encoded PKCS8 private key into the Kubo keystore.
func (k *KuboClient) KeyImport(name, pemBody string) error {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", "key.pem")
	if err != nil {
		return fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write([]byte(pemBody)); err != nil {
		return fmt.Errorf("write pem data: %w", err)
	}
	w.Close()

	url := fmt.Sprintf("%s/key/import?arg=%s&format=pem-pkcs8-cleartext", k.apiURL, name)
	resp, err := k.client.Post(url, w.FormDataContentType(), &buf)
	if err != nil {
		return fmt.Errorf("ipfs key/import: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("ipfs key/import: status %d", resp.StatusCode)
	}
	return nil
}

// NamePublish publishes a CID under an IPNS name using the given key.
func (k *KuboClient) NamePublish(cid, keyName string) error {
	c := &http.Client{Timeout: 60 * time.Second}
	url := fmt.Sprintf("%s/name/publish?arg=/ipfs/%s&key=%s", k.apiURL, cid, keyName)
	resp, err := c.Post(url, "", nil)
	if err != nil {
		return fmt.Errorf("ipfs name/publish: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("ipfs name/publish: status %d", resp.StatusCode)
	}
	return nil
}

// NameResolve resolves an IPNS name to a CID (without /ipfs/ prefix).
func (k *KuboClient) NameResolve(ipnsName string) (string, error) {
	c := &http.Client{Timeout: 30 * time.Second}
	resp, err := c.Post(k.apiURL+"/name/resolve?arg="+ipnsName, "", nil)
	if err != nil {
		return "", fmt.Errorf("ipfs name/resolve: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ipfs name/resolve: status %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Path string `json:"Path"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("ipfs name/resolve: parse: %w", err)
	}
	return strings.TrimPrefix(result.Path, "/ipfs/"), nil
}
