//go:build linux

package eventloop

// Single-threaded, epoll-driven event loop.
//
// The model: one goroutine owns one epoll(7) instance and every connection
// fd. It never issues a blocking read or write itself — instead it asks the
// kernel "wake me when one of these fds is ready" (epoll_wait), and only
// then touches a socket, and only with a non-blocking read/write that is
// guaranteed not to stall it. That's what lets a single goroutine hold open
// many thousands of idle-or-slow client connections at once: there's no
// per-connection goroutine (and its ~2-8KB starting stack) or scheduler
// entry sitting around waiting for those clients to say something.
//
// This deliberately bypasses Go's net.Conn: net.Conn's Read/Write already
// go through the Go runtime's own internal netpoller (which is itself
// epoll-based on Linux) to give goroutines the illusion of blocking I/O.
// Layering our epoll loop on top of that would mean two epoll instances
// doing the same job. Instead, once we have a connection's raw fd we drop
// down to the syscall package entirely (Accept4/Read/Write/Close) and
// register that fd with our own epoll instance, so there's exactly one
// readiness mechanism in play.
//
// Level-triggered vs. edge-triggered: this uses the epoll default,
// level-triggered (LT) mode — epoll keeps reporting a fd as ready every
// time epoll_wait is called for as long as there's unread data / write
// buffer space, rather than only once when it *becomes* ready
// (edge-triggered, EPOLLET). ET can save a few syscalls under heavy load,
// but only if the reader is disciplined about always looping a socket to
// EAGAIN before moving on — one missed drain and a connection silently
// stalls forever. LT is more forgiving (a fd we didn't fully drain just
// gets reported again next time) at the cost of a possible redundant wakeup,
// which is the right trade for a first epoll implementation. This code
// happens to drain every readable fd to EAGAIN anyway (see handleReadable),
// so switching to ET later would be a small, low-risk change if profiling
// ever calls for it.
//
// RESP framing across readiness events: a client's command can arrive
// split across multiple TCP packets/reads (e.g. a large bulk string, or
// just an unlucky packet boundary), and a non-blocking read only ever
// returns "whatever's arrived so far" — it can't wait for the rest. So each
// connection keeps its own read buffer (conn.readBuf) that newly-read bytes
// are appended to; after every read we try to decode a RESP value from the
// front of that buffer via the same resp.Reader every other part of this
// project uses, wrapped around a bytes.Reader over the buffered bytes. Two
// outcomes: decode fails with EOF/ErrUnexpectedEOF (ran out of bytes
// mid-value) — the command isn't fully here yet, stop and wait for the next
// EPOLLIN; or it succeeds — resp.Reader.Buffered() tells us exactly how
// many bytes of the buffer that value consumed (see its doc comment), so we
// advance the buffer past it and, since a client can pipeline several
// commands into one packet, immediately try to decode another. Writes get
// the mirror-image treatment: a reply can be too big to write in one
// non-blocking syscall, so pending output lives in conn.writeBuf and
// EPOLLOUT is registered on the fd only while there's a backlog to flush,
// then dropped again once it drains (level-triggered epoll would otherwise
// keep reporting an always-writable fd as ready forever, burning CPU on
// wakeups with nothing to do).
import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"syscall"

	"github.com/Ishan819/Redis-clone/internal/command"
	"github.com/Ishan819/Redis-clone/internal/resp"
	"github.com/Ishan819/Redis-clone/internal/store"
)

// readChunkSize is how many bytes we ask the kernel for per Read syscall.
// It's just a scratch buffer size, not a limit on command size — data that
// doesn't fit is simply appended to conn.readBuf across multiple reads.
const readChunkSize = 4096

// maxEpollEvents bounds how many ready fds epoll_wait reports per call. A
// busy fd that's still ready gets reported again on the next call, so this
// only caps how much work one iteration of the loop takes, not throughput.
const maxEpollEvents = 128

// conn holds the event loop's per-connection state. Everything here is
// only ever touched by the single loop goroutine, so — unlike
// internal/store, which is shared across goroutines and needs its own
// mutex — none of this needs locking.
type conn struct {
	fd int

	// readBuf accumulates bytes read from fd that haven't yet formed a
	// complete RESP value. bytes.Buffer is a convenient fit: Write appends
	// newly-read bytes, and Next(n) discards exactly the prefix a
	// successful decode consumed while compacting the rest forward, which
	// is exactly the "streaming parse" access pattern this needs.
	readBuf bytes.Buffer

	// writeBuf holds reply bytes not yet accepted by the kernel. Flushed
	// opportunistically after every command dispatch and again whenever
	// EPOLLOUT fires.
	writeBuf bytes.Buffer

	// wantWrite tracks whether fd is currently registered for EPOLLOUT, so
	// setWriteInterest only issues an epoll_ctl MOD when the desired state
	// actually changes.
	wantWrite bool
}

// loop is the epoll event loop's state: the epoll instance itself plus the
// set of connections it's currently multiplexing.
type loop struct {
	epfd  int
	store *store.Store
	conns map[int]*conn
}

// Serve runs a single-threaded epoll event loop over ln's connections,
// dispatching decoded commands against s. It blocks until epoll_wait
// returns an unrecoverable error (e.g. the listener's fd is torn down), at
// which point it returns that error, mirroring Serve's contract in
// goroutine_other.go.
func Serve(ln net.Listener, s *store.Store) error {
	listenFD, err := listenerFD(ln)
	if err != nil {
		return fmt.Errorf("eventloop: %w", err)
	}

	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		return fmt.Errorf("eventloop: epoll_create1: %w", err)
	}
	defer syscall.Close(epfd)

	l := &loop{epfd: epfd, store: s, conns: make(map[int]*conn)}

	if err := epollAdd(epfd, listenFD, syscall.EPOLLIN); err != nil {
		return fmt.Errorf("eventloop: registering listener with epoll: %w", err)
	}

	events := make([]syscall.EpollEvent, maxEpollEvents)
	for {
		n, err := syscall.EpollWait(epfd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue // interrupted by a signal; not a real failure
			}
			return fmt.Errorf("eventloop: epoll_wait: %w", err)
		}

		for i := 0; i < n; i++ {
			ev := events[i]
			fd := int(ev.Fd)

			if fd == listenFD {
				l.acceptAll(listenFD)
				continue
			}

			c, ok := l.conns[fd]
			if !ok {
				// Stale event for a fd we've already closed and removed
				// (e.g. both EPOLLIN and EPOLLHUP were pending for the same
				// fd in one epoll_wait batch and an earlier event in this
				// same batch already closed it). Safe to ignore.
				continue
			}

			if ev.Events&(syscall.EPOLLHUP|syscall.EPOLLERR) != 0 {
				l.closeConn(c)
				continue
			}
			if ev.Events&syscall.EPOLLIN != 0 {
				if !l.handleReadable(c) {
					continue // handleReadable already closed c
				}
			}
			if ev.Events&syscall.EPOLLOUT != 0 {
				l.flushWrite(c)
			}
		}
	}
}

// listenerFD extracts ln's underlying socket file descriptor so this
// package can drive it directly with syscalls instead of net.Listener's
// (runtime-netpoller-backed) Accept. rc.Control runs the callback
// synchronously on the fd before any concurrent close, which is all the
// safety we need here since we read the fd once at startup and then own
// the listener for the lifetime of the process.
func listenerFD(ln net.Listener) (int, error) {
	sc, ok := ln.(syscall.Conn)
	if !ok {
		return 0, fmt.Errorf("listener %T does not support SyscallConn", ln)
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("SyscallConn: %w", err)
	}

	var fd int
	var ctrlErr error
	err = rc.Control(func(f uintptr) {
		fd = int(f)
		// Go already puts listener sockets in non-blocking mode for its
		// own netpoller's benefit; this is a defensive, idempotent
		// belt-and-suspenders call so Serve's correctness never silently
		// depends on that runtime implementation detail.
		ctrlErr = syscall.SetNonblock(fd, true)
	})
	if err != nil {
		return 0, fmt.Errorf("Control: %w", err)
	}
	if ctrlErr != nil {
		return 0, fmt.Errorf("SetNonblock: %w", ctrlErr)
	}
	return fd, nil
}

// acceptAll drains every connection currently pending on the listener.
// Level-triggered epoll only promises "at least one" is ready, and more may
// have queued up by the time we get around to Accept4-ing them, so this
// loops until the kernel says there are none left (EAGAIN) rather than
// accepting just one per EPOLLIN notification.
func (l *loop) acceptAll(listenFD int) {
	for {
		nfd, _, err := syscall.Accept4(listenFD, syscall.SOCK_NONBLOCK)
		if err != nil {
			if err == syscall.EAGAIN {
				return // no more pending connections
			}
			if err == syscall.ECONNABORTED {
				continue // peer aborted before we got to it; try the next
			}
			log.Printf("redis-clone: accept: %v", err)
			return
		}

		if err := epollAdd(l.epfd, nfd, syscall.EPOLLIN); err != nil {
			log.Printf("redis-clone: registering connection fd %d with epoll: %v", nfd, err)
			syscall.Close(nfd)
			continue
		}
		l.conns[nfd] = &conn{fd: nfd}
	}
}

// handleReadable drains fd into c.readBuf, then decodes and dispatches as
// many complete RESP commands as are now buffered, flushing replies before
// returning. It reports whether c is still open (false means it already
// closed and removed c — callers must not touch c again).
func (l *loop) handleReadable(c *conn) bool {
	buf := make([]byte, readChunkSize)
readLoop:
	for {
		n, err := syscall.Read(c.fd, buf)
		switch {
		case err == syscall.EAGAIN:
			// Drained everything the kernel currently has for us.
			break readLoop
		case err != nil:
			l.closeConn(c)
			return false
		case n == 0:
			// Read of 0 bytes with no error means the peer closed its
			// write side (the TCP equivalent of net.Conn.Read's io.EOF).
			l.closeConn(c)
			return false
		}
		c.readBuf.Write(buf[:n])
	}

	if !l.processBuffered(c) {
		return false // a malformed command already closed c
	}
	l.flushWrite(c)
	return true
}

// processBuffered decodes and dispatches every complete RESP command
// currently sitting in c.readBuf, stopping when what's left is only a
// partial command (waiting on more bytes from a future read) or the buffer
// is empty. It reports whether c is still open.
func (l *loop) processBuffered(c *conn) bool {
	for {
		data := c.readBuf.Bytes()
		if len(data) == 0 {
			return true
		}

		r := resp.NewReader(bytes.NewReader(data))
		val, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// The buffer holds a real prefix of a value but not all of
				// it yet (e.g. a bulk string whose declared length hasn't
				// all arrived). Leave readBuf untouched and wait for the
				// next EPOLLIN to bring the rest.
				return true
			}
			// Anything else is a genuine RESP protocol violation, not a
			// framing gap — mirrors the blocking server's behavior of
			// closing the connection on a decode error rather than trying
			// to resynchronize with a client that isn't speaking RESP.
			c.writeBuf.Write(errorReply(err))
			l.flushWrite(c)
			l.closeConn(c)
			return false
		}

		// r.Buffered() is how much of `data` resp.Reader read into its
		// internal bufio buffer but didn't end up consuming for this one
		// value; the rest was consumed decoding val.
		consumed := len(data) - r.Buffered()
		c.readBuf.Next(consumed)

		l.dispatch(c, val)
	}
}

// dispatch runs one already-decoded command against the store and appends
// its RESP reply to c.writeBuf. It never itself does I/O — the caller
// decides when to flush, so several pipelined commands can be answered with
// one write.
func (l *loop) dispatch(c *conn, val resp.Value) {
	args, err := toArgs(val)
	if err != nil {
		c.writeBuf.Write(errorReply(err))
		return
	}
	if len(args) == 0 {
		return
	}

	handler := command.Lookup(args[0])
	if handler == nil {
		c.writeBuf.Write(unknownCommandReply(args[0]))
		return
	}

	reply := handler(l.store, args[1:])
	c.writeBuf.Write(reply.Marshal())
}

// flushWrite pushes as much of c.writeBuf to the kernel as a non-blocking
// write will currently accept. If the kernel's socket buffer fills up
// before c.writeBuf drains, it registers EPOLLOUT so the loop resumes
// flushing once there's room; if c.writeBuf fully drains, it makes sure
// EPOLLOUT is *not* registered, since a level-triggered "always writable"
// fd would otherwise fire on every single epoll_wait for no reason.
func (l *loop) flushWrite(c *conn) {
	for c.writeBuf.Len() > 0 {
		n, err := syscall.Write(c.fd, c.writeBuf.Bytes())
		if err != nil {
			if err == syscall.EAGAIN {
				l.setWriteInterest(c, true)
				return
			}
			l.closeConn(c)
			return
		}
		c.writeBuf.Next(n)
	}
	l.setWriteInterest(c, false)
}

// setWriteInterest adds or drops EPOLLOUT from fd's epoll registration,
// skipping the epoll_ctl syscall entirely when the state isn't changing.
func (l *loop) setWriteInterest(c *conn, want bool) {
	if want == c.wantWrite {
		return
	}
	events := uint32(syscall.EPOLLIN)
	if want {
		events |= syscall.EPOLLOUT
	}
	if err := epollMod(l.epfd, c.fd, events); err != nil {
		log.Printf("redis-clone: updating epoll interest for fd %d: %v", c.fd, err)
		return
	}
	c.wantWrite = want
}

// closeConn deregisters fd from epoll, closes it, and forgets c. Safe to
// call even if fd was already half-torn-down by the peer.
func (l *loop) closeConn(c *conn) {
	// EPOLL_CTL_DEL's event argument is ignored by the kernel but required
	// to be non-nil on some historical kernels; pass a zero value.
	syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_DEL, c.fd, &syscall.EpollEvent{})
	syscall.Close(c.fd)
	delete(l.conns, c.fd)
}

// epollAdd registers fd for events (EPOLL_CTL_ADD).
func epollAdd(epfd, fd int, events uint32) error {
	return syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
}

// epollMod changes the event set already registered for fd (EPOLL_CTL_MOD).
func epollMod(epfd, fd int, events uint32) error {
	return syscall.EpollCtl(epfd, syscall.EPOLL_CTL_MOD, fd, &syscall.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
}
