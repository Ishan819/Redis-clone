// Package server implements the TCP front end of the redis-clone process:
// it accepts client connections, decodes RESP commands off the wire, and
// dispatches them to the command package.
//
// Phase 1 uses one goroutine per connection, the simplest thing that works.
// A later phase replaces the accept/read loop here with an epoll-based
// event loop without changing the command package at all.
package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/Ishan819/Redis-clone/internal/command"
	"github.com/Ishan819/Redis-clone/internal/resp"
	"github.com/Ishan819/Redis-clone/internal/store"
)

// activeExpiryInterval is how often the server sweeps a sample of
// keys-with-TTL for expired entries, matching the cadence of Redis's own
// default active expire cycle (10 times per second).
const activeExpiryInterval = 100 * time.Millisecond

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

	stopActiveExpiry := s.store.StartActiveExpiry(activeExpiryInterval)
	defer stopActiveExpiry()

	log.Printf("redis-clone listening on %s", s.Addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("server: accept: %w", err)
		}
		go s.handleConn(conn)
	}
}

// handleConn serves one client connection until it disconnects or sends
// something this server can't parse.
func (s *Server) handleConn(conn net.Conn) {
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
			writeErr(conn, err)
			continue
		}
		if len(args) == 0 {
			continue
		}

		handler := command.Lookup(args[0])
		if handler == nil {
			writeErr(conn, fmt.Errorf("ERR unknown command '%s'", args[0]))
			continue
		}

		reply := handler(s.store, args[1:])
		if _, err := conn.Write(reply.Marshal()); err != nil {
			log.Printf("redis-clone: write error: %v", err)
			return
		}
	}
}

// toArgs converts an incoming RESP value into a command name plus its
// arguments. Clients (redis-cli included) send commands as RESP arrays of
// bulk strings.
func toArgs(v resp.Value) ([]string, error) {
	if v.Type != resp.Array {
		return nil, fmt.Errorf("ERR expected a RESP array of bulk strings, got type %q", byte(v.Type))
	}
	args := make([]string, len(v.Array))
	for i, elem := range v.Array {
		if elem.Type != resp.BulkString {
			return nil, fmt.Errorf("ERR expected a bulk string in command array")
		}
		args[i] = elem.Str
	}
	return args, nil
}

func writeErr(w io.Writer, err error) {
	w.Write(resp.ErrorValue(err.Error()).Marshal())
}
