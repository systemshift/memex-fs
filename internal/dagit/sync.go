package dagit

import (
	"log"
	"time"
)

// FeedSyncer periodically polls followed feeds in the background.
type FeedSyncer struct {
	fm       *FeedManager
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewFeedSyncer creates a syncer that polls at the given interval.
func NewFeedSyncer(fm *FeedManager, interval time.Duration) *FeedSyncer {
	return &FeedSyncer{
		fm:       fm,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start launches the background polling goroutine.
func (s *FeedSyncer) Start() {
	go func() {
		defer close(s.doneCh)
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				if !s.fm.kubo.IsAvailable() {
					continue
				}
				summary, err := s.fm.CheckFeeds()
				if err != nil {
					log.Printf("memex-fs: feed sync error: %v", err)
				} else if summary != "Not following anyone." {
					log.Printf("memex-fs: feed sync: %s", summary)
				}
			}
		}
	}()
}

// Stop signals the syncer to stop and waits for it to finish.
func (s *FeedSyncer) Stop() {
	close(s.stopCh)
	<-s.doneCh
}
