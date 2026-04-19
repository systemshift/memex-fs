package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/systemshift/memex-fs/internal/dag"
	"github.com/systemshift/memex-fs/internal/dagit"
	memexfuse "github.com/systemshift/memex-fs/internal/fuse"
)

func main() {
	// Dispatch on the first argument. Anything that doesn't match a known
	// subcommand falls through to mount, so `memex-fs --mount /path` keeps
	// working.
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "push":
			runPush(os.Args[2:])
			return
		case "pull":
			runPull(os.Args[2:])
			return
		case "mount":
			runMount(os.Args[2:])
			return
		case "-h", "--help":
			printUsage()
			return
		}
	}

	// Default: mount, consuming all flags.
	runMount(os.Args[1:])
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: memex-fs <command> [flags]

Commands:
  mount     Mount the repo as a FUSE filesystem (default)
  push      Upload every object reachable from HEAD to IPFS
  pull      Fetch a commit CID and its reachable objects from IPFS

Run 'memex-fs <command> -h' for command-specific flags.
`)
}

func runMount(args []string) {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	var (
		dataDir    = fs.String("data", ".", "Data directory (contains .mx/)")
		mountpoint = fs.String("mount", "", "FUSE mount point (required)")
		debug      = fs.Bool("debug", false, "Enable FUSE debug logging")
	)
	fs.Parse(args)

	if *mountpoint == "" {
		log.Fatal("memex-fs: --mount is required")
	}

	if err := os.MkdirAll(*mountpoint, 0755); err != nil {
		log.Fatalf("memex-fs: create mountpoint: %v", err)
	}

	log.Printf("memex-fs: opening repository at %s", *dataDir)
	repo, err := dag.OpenRepository(*dataDir)
	if err != nil {
		log.Fatalf("memex-fs: failed to open repository: %v", err)
	}

	log.Printf("memex-fs: mounting at %s", *mountpoint)
	server, err := memexfuse.MountFS(*mountpoint, repo, *debug)
	if err != nil {
		log.Fatalf("memex-fs: mount failed: %v", err)
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-done
		log.Println("memex-fs: shutting down...")
		server.Unmount()
	}()

	log.Printf("memex-fs: ready (pid %d)", os.Getpid())
	server.Wait()
	log.Println("memex-fs: stopped")
}

// runPush uploads every object reachable from HEAD to IPFS. With --publish
// it also imports the repo's Ed25519 identity into the Kubo keystore (if
// not already) and publishes the HEAD CID under an IPNS name derived from
// the identity's DID. Prints the HEAD CID on success, and the IPNS name
// when publishing.
func runPush(args []string) {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	var (
		dataDir = fs.String("data", ".", "Data directory (contains .mx/)")
		kuboAPI = fs.String("kubo-api", "http://localhost:5001/api/v0", "Kubo API URL")
		publish = fs.Bool("publish", false, "Publish HEAD CID over IPNS under the repo's identity")
	)
	fs.Parse(args)

	repo, err := dag.OpenRepository(*dataDir)
	if err != nil {
		log.Fatalf("memex-fs push: open repository: %v", err)
	}

	kubo := dagit.NewKuboClient(*kuboAPI)
	if !kubo.IsAvailable() {
		log.Fatalf("memex-fs push: Kubo not available at %s", *kuboAPI)
	}

	headCID, err := dagit.Push(repo, kubo)
	if err != nil {
		log.Fatalf("memex-fs push: %v", err)
	}
	fmt.Println(headCID)

	if *publish {
		identity, err := dag.LoadIdentity()
		if err != nil {
			log.Fatalf("memex-fs push: load identity: %v", err)
		}
		if err := dagit.EnsureKey(kubo, identity, dagit.HeadKeyName); err != nil {
			log.Fatalf("memex-fs push: key import: %v", err)
		}
		if err := kubo.NamePublish(headCID, dagit.HeadKeyName); err != nil {
			log.Fatalf("memex-fs push: IPNS publish: %v", err)
		}
		ipnsName, err := dagit.DIDToIPNSName(identity.DID)
		if err != nil {
			log.Fatalf("memex-fs push: derive IPNS name: %v", err)
		}
		fmt.Fprintf(os.Stderr, "memex-fs: published as ipns://%s (did %s)\n", ipnsName, identity.DID)
	}
}

// runPull fetches a commit and everything reachable from it into the local
// ObjectStore. Accepts either a commit CID (base32, e.g. bafk...) or a
// did:key DID, which is resolved via IPNS to the DID holder's latest
// published HEAD CID. Does not update refs or HEAD — browse via /at/{cid}/.
func runPull(args []string) {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	var (
		dataDir = fs.String("data", ".", "Data directory (contains .mx/)")
		kuboAPI = fs.String("kubo-api", "http://localhost:5001/api/v0", "Kubo API URL")
	)
	fs.Parse(args)

	if fs.NArg() < 1 {
		log.Fatal("memex-fs pull: missing CID or DID argument")
	}
	source := fs.Arg(0)

	repo, err := dag.OpenRepository(*dataDir)
	if err != nil {
		log.Fatalf("memex-fs pull: open repository: %v", err)
	}

	kubo := dagit.NewKuboClient(*kuboAPI)
	if !kubo.IsAvailable() {
		log.Fatalf("memex-fs pull: Kubo not available at %s", *kuboAPI)
	}

	headCID := source
	if strings.HasPrefix(source, "did:key:") {
		ipnsName, err := dagit.DIDToIPNSName(source)
		if err != nil {
			log.Fatalf("memex-fs pull: invalid DID: %v", err)
		}
		resolved, err := kubo.NameResolve(ipnsName)
		if err != nil {
			log.Fatalf("memex-fs pull: IPNS resolve: %v", err)
		}
		headCID = resolved
		fmt.Fprintf(os.Stderr, "memex-fs: resolved %s -> %s\n", source, headCID)
	}

	if err := dagit.Pull(repo, kubo, headCID); err != nil {
		log.Fatalf("memex-fs pull: %v", err)
	}
	fmt.Fprintf(os.Stderr, "memex-fs: pulled %s; browse at /at/%s/ on a mounted repo\n", headCID, headCID)
}
