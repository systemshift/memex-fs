package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
		dataDir      = fs.String("data", ".", "Data directory (contains .mx/)")
		mountpoint   = fs.String("mount", "", "FUSE mount point (required)")
		kuboAPI      = fs.String("kubo-api", "http://localhost:5001/api/v0", "Kubo API URL")
		feedInterval = fs.String("feed-interval", "5m", "Background feed sync interval")
		noFeeds      = fs.Bool("no-feeds", false, "Disable feeds/dagit integration")
		debug        = fs.Bool("debug", false, "Enable FUSE debug logging")
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

	var fm *dagit.FeedManager
	var syncer *dagit.FeedSyncer

	if !*noFeeds {
		identity, err := dag.LoadIdentity()
		if err != nil {
			log.Printf("memex-fs: identity warning: %v (feeds disabled)", err)
		} else {
			kubo := dagit.NewKuboClient(*kuboAPI)
			fm = dagit.NewFeedManager(kubo, identity, repo)

			if kubo.IsAvailable() {
				if err := fm.EnsureKey(); err != nil {
					log.Printf("memex-fs: key import warning: %v", err)
				}
			} else {
				log.Printf("memex-fs: Kubo not available at %s (feeds will work when Kubo starts)", *kuboAPI)
			}

			interval, err := time.ParseDuration(*feedInterval)
			if err != nil {
				interval = 5 * time.Minute
			}
			syncer = dagit.NewFeedSyncer(fm, interval)
			syncer.Start()
			log.Printf("memex-fs: feed syncer started (interval %s)", interval)
		}
	}

	log.Printf("memex-fs: mounting at %s", *mountpoint)
	server, err := memexfuse.MountFS(*mountpoint, repo, fm, *debug)
	if err != nil {
		log.Fatalf("memex-fs: mount failed: %v", err)
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-done
		log.Println("memex-fs: shutting down...")
		if syncer != nil {
			syncer.Stop()
		}
		server.Unmount()
	}()

	log.Printf("memex-fs: ready (pid %d)", os.Getpid())
	server.Wait()
	log.Println("memex-fs: stopped")
}

// runPush uploads every object reachable from HEAD to IPFS. Prints the HEAD
// CID on success so the user can share it (e.g. to a peer running pull).
func runPush(args []string) {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	var (
		dataDir = fs.String("data", ".", "Data directory (contains .mx/)")
		kuboAPI = fs.String("kubo-api", "http://localhost:5001/api/v0", "Kubo API URL")
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
}

// runPull fetches a commit CID and everything reachable from it into the
// local ObjectStore. Does not update refs or HEAD — browse the pulled
// snapshot via /at/{cid}/ on a mounted repo.
func runPull(args []string) {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	var (
		dataDir = fs.String("data", ".", "Data directory (contains .mx/)")
		kuboAPI = fs.String("kubo-api", "http://localhost:5001/api/v0", "Kubo API URL")
	)
	fs.Parse(args)

	if fs.NArg() < 1 {
		log.Fatal("memex-fs pull: missing commit CID argument")
	}
	headCID := fs.Arg(0)

	repo, err := dag.OpenRepository(*dataDir)
	if err != nil {
		log.Fatalf("memex-fs pull: open repository: %v", err)
	}

	kubo := dagit.NewKuboClient(*kuboAPI)
	if !kubo.IsAvailable() {
		log.Fatalf("memex-fs pull: Kubo not available at %s", *kuboAPI)
	}

	if err := dagit.Pull(repo, kubo, headCID); err != nil {
		log.Fatalf("memex-fs pull: %v", err)
	}
	fmt.Fprintf(os.Stderr, "memex-fs: pulled %s; browse at /at/%s/ on a mounted repo\n", headCID, headCID)
}
