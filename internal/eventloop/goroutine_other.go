//go:build !linux

package eventloop

// This file is the portable fallback used on every OS except Linux — most
// relevantly macOS, since that's where this project is developed. epoll is
// a Linux-only syscall family (macOS/BSD have their own equivalent, kqueue,
// which this project doesn't implement); rather than block development on a
// second readiness API, non-Linux keeps the simple, well-understood model
// this project started with: one goroutine per connection, blocking reads.
// The Go scheduler multiplexes those goroutines onto a handful of OS
// threads for us, so it scales further than it looks like it should, just
// not as far as the single-thread-plus-epoll design in epoll_linux.go, and
// with per-connection stack + scheduling overhead that design avoids.

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/Ishan819/Redis-clone/internal/command"
	"github.com/Ishan819/Redis-clone/internal/resp"
	"github.com/Ishan819/Redis-clone/internal/store"
)

// Serve accepts connections on ln and serves each on its own goroutine
// until Accept fails (e.g. because ln was closed), matching the
// goroutine-per-connection model Phases 1-7 used directly in
// internal/server.
func Serve(ln net.Listener, s *store.Store) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("eventloop: accept: %w", err)
		}
		go handleConn(conn, s)
	}
}

// handleConn serves one client connection until it disconnects or sends
// something this server can't parse. Reads are blocking: resp.Reader pulls
// directly from conn, and the goroutine simply parks (at no CPU cost, via
// the Go runtime's own netpoller) whenever there isn't a full message to
// read yet — the same partial-command-across-packets case epoll_linux.go
// has to handle explicitly with its own buffering, handled here for free
// by bufio.Reader blocking until more bytes arrive.
func handleConn(conn net.Conn, s *store.Store) {
	defer conn.Close()
	reader := resp.NewReader(conn)

	for {
		req, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("redis-clone: connection error: %v", err)
			}
			return
		}

		args, err := toArgs(req)
		if err != nil {
			conn.Write(errorReply(err))
			continue
		}
		if len(args) == 0 {
			continue
		}

		handler := command.Lookup(args[0])
		if handler == nil {
			conn.Write(unknownCommandReply(args[0]))
			continue
		}

		reply := handler(s, args[1:])
		if _, err := conn.Write(reply.Marshal()); err != nil {
			log.Printf("redis-clone: write error: %v", err)
			return
		}
	}
}
