// Package server implements the TCP front end of the redis-clone process:
// it owns the listener's lifecycle (accepting a snapshot on startup,
// running background expiry and periodic snapshotting) and hands the
// listener off to internal/eventloop for the actual accept/read/dispatch
// loop. Phase 8 split that loop into its own package specifically so it
// could have two implementations — an epoll-based event loop on Linux, a
// goroutine-per-connection fallback everywhere else — selected by build
// tag without this package needing to know or care which one it's running;
// see internal/eventloop's package doc for the design.
package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/Ishan819/Redis-clone/internal/eventloop"
	"github.com/Ishan819/Redis-clone/internal/store"
)

// activeExpiryInterval is how often the server sweeps a sample of
// keys-with-TTL for expired entries, matching the cadence of Redis's own
// default active expire cycle (10 times per second).
const activeExpiryInterval = 100 * time.Millisecond

// snapshotInterval is how often the server saves a full RDB-style
// snapshot to disk in the background, independent of any explicit
// SAVE/BGSAVE from a client. Real Redis only saves when its configured
// "save points" (e.g. "at least 1 key changed in 60s") are met; this is a
// simpler, unconditional periodic save, which is easier to reason about
// at the cost of occasionally writing an unchanged dataset.
const snapshotInterval = 60 * time.Second

// Server is a minimal RESP-speaking TCP server. All connections share a
// single in-memory Store.
type Server struct {
	Addr  string
	store *store.Store
}

// New returns a Server that will listen on addr (e.g. ":6379") once
// ListenAndServe is called.
func New(addr string) *Server {
	return &Server{Addr: addr, store: store.New()}
}

// ListenAndServe binds Addr and serves connections until the listener
// fails (it never returns nil).
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("server: listen on %s: %w", s.Addr, err)
	}
	defer ln.Close()

	s.loadSnapshot()

	stopActiveExpiry := s.store.StartActiveExpiry(activeExpiryInterval)
	defer stopActiveExpiry()

	stopSnapshotting := s.startPeriodicSnapshot(snapshotInterval)
	defer stopSnapshotting()

	log.Printf("redis-clone listening on %s", s.Addr)

	return eventloop.Serve(ln, s.store)
}

// loadSnapshot loads store.DefaultRDBPath into the server's store, if it
// exists, so a restarted server picks up where the last SAVE/BGSAVE (or
// periodic snapshot) left off. A missing file — the normal case on a
// first run — is logged and otherwise ignored rather than treated as an
// error.
func (s *Server) loadSnapshot() {
	err := s.store.LoadFromFile(store.DefaultRDBPath)
	switch {
	case err == nil:
		log.Printf("redis-clone: loaded snapshot from %s", store.DefaultRDBPath)
	case errors.Is(err, os.ErrNotExist):
		log.Printf("redis-clone: no snapshot found at %s, starting with an empty dataset", store.DefaultRDBPath)
	default:
		log.Printf("redis-clone: failed to load snapshot from %s: %v (starting with an empty dataset)", store.DefaultRDBPath, err)
	}
}

// startPeriodicSnapshot launches a background goroutine that saves a full
// snapshot to store.DefaultRDBPath every interval, so the dataset
// survives a restart even without an explicit SAVE/BGSAVE. It returns a
// stop function that halts the goroutine.
func (s *Server) startPeriodicSnapshot(interval time.Duration) (stop func()) {
	stopCh := make(chan struct{})
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if err := s.store.SaveToFile(store.DefaultRDBPath); err != nil {
					log.Printf("redis-clone: periodic snapshot failed: %v", err)
				}
			}
		}
	}()
	return func() { close(stopCh) }
}
