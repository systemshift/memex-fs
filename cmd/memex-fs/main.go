package main

import (
	"flag"
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
	var (
		dataDir      string
		mountpoint   string
		kuboAPI      string
		feedInterval string
		noFeeds      bool
	)

	flag.StringVar(&dataDir, "data", ".", "Data directory (contains .mx/)")
	flag.StringVar(&mountpoint, "mount", "", "FUSE mount point (required)")
	flag.StringVar(&kuboAPI, "kubo-api", "http://localhost:5001/api/v0", "Kubo API URL")
	flag.StringVar(&feedInterval, "feed-interval", "5m", "Background feed sync interval")
	flag.BoolVar(&noFeeds, "no-feeds", false, "Disable feeds/dagit integration")
	flag.Parse()

	if mountpoint == "" {
		log.Fatal("memex-fs: --mount is required")
	}

	// Ensure mountpoint exists
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		log.Fatalf("memex-fs: create mountpoint: %v", err)
	}

	log.Printf("memex-fs: opening repository at %s", dataDir)
	repo, err := dag.OpenRepository(dataDir)
	if err != nil {
		log.Fatalf("memex-fs: failed to open repository: %v", err)
	}

	// Set up feed manager (optional)
	var fm *dagit.FeedManager
	var syncer *dagit.FeedSyncer

	if !noFeeds {
		identity, err := dag.LoadIdentity()
		if err != nil {
			log.Printf("memex-fs: identity warning: %v (feeds disabled)", err)
		} else {
			kubo := dagit.NewKuboClient(kuboAPI)
			fm = dagit.NewFeedManager(kubo, identity, repo)

			// Try to import key into Kubo (non-fatal if Kubo not running)
			if kubo.IsAvailable() {
				if err := fm.EnsureKey(); err != nil {
					log.Printf("memex-fs: key import warning: %v", err)
				}
			} else {
				log.Printf("memex-fs: Kubo not available at %s (feeds will work when Kubo starts)", kuboAPI)
			}

			// Start background syncer
			interval, err := time.ParseDuration(feedInterval)
			if err != nil {
				interval = 5 * time.Minute
			}
			syncer = dagit.NewFeedSyncer(fm, interval)
			syncer.Start()
			log.Printf("memex-fs: feed syncer started (interval %s)", interval)
		}
	}

	log.Printf("memex-fs: mounting at %s", mountpoint)
	server, err := memexfuse.MountFS(mountpoint, repo, fm)
	if err != nil {
		log.Fatalf("memex-fs: mount failed: %v", err)
	}

	// Unmount on signal
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
