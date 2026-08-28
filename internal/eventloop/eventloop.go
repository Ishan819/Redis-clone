// Package eventloop owns the accept/read/dispatch loop that turns bytes on
// a TCP connection into command.Handler calls and RESP replies. It used to
// live directly in internal/server as one goroutine per connection; Phase 8
// splits it out and gives it two implementations, selected at compile time
// by build tag on GOOS:
//
//   - epoll_linux.go (linux): a single-threaded, non-blocking event loop
//     built directly on the epoll(7) syscalls. One goroutine owns an epoll
//     instance and every connection's raw socket fd; readiness
//     notifications (EPOLLIN/EPOLLOUT) drive reads, RESP parsing/dispatch,
//     and writes, and that goroutine never blocks waiting on a slow or idle
//     client. This is what lets it hold many concurrent connections without
//     a per-connection goroutine (and the stack + scheduler overhead that
//     implies).
//   - goroutine_other.go (!linux, e.g. macOS during development): epoll is
//     a Linux-only facility, so every other OS falls back to the original
//     goroutine-per-connection model — one goroutine per client, blocking
//     reads via resp.Reader, exactly what Phases 1-7 used.
//
// Both files export the same func Serve(ln net.Listener, s *store.Store)
// error, so internal/server calls one name and gets whichever
// implementation the build tag selected; from a client's point of view the
// two are indistinguishable — same commands, same RESP framing, same error
// behavior. This file holds the pieces both implementations share so that
// equivalence isn't just a comment, it's shared code.
package eventloop

import (
	"fmt"

	"github.com/Ishan819/Redis-clone/internal/resp"
)

// toArgs converts a decoded RESP value into a command name plus its
// arguments. Clients (redis-cli included) send commands as RESP arrays of
// bulk strings; anything else is a protocol-level mistake by the client,
// not a signal to reset the connection, so it's reported as a RESP error
// reply rather than a fatal decode error.
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

// errorReply marshals err as a RESP error reply, ready to write straight to
// a connection.
func errorReply(err error) []byte {
	return resp.ErrorValue(err.Error()).Marshal()
}

// unknownCommandReply builds the RESP error reply for a command name with
// no registered Handler.
func unknownCommandReply(name string) []byte {
	return resp.Errorf("ERR unknown command '%s'", name).Marshal()
}
