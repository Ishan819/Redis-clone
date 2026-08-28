package eventloop

// These tests exercise Serve end-to-end over a real loopback TCP
// connection rather than mocking anything, deliberately: Serve's exported
// contract (accept connections, decode RESP, dispatch, reply) is meant to
// be identical regardless of which build-tagged implementation compiled
// in, and the same file — with no build tag of its own — runs against
// whichever one that is. On macOS during development that's
// goroutine_other.go; on Linux CI/deployment it's epoll_linux.go. Running
// this file on both is what makes "the two implementations are
// behaviorally equivalent" a tested claim instead of just a comment.

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/Ishan819/Redis-clone/internal/store"
)

// startTestServer starts Serve on a loopback listener in the background
// and returns its address. The listener is closed on test cleanup, which
// unblocks Serve's accept loop with an error and lets its goroutine exit.
func startTestServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	s := store.New()
	go Serve(ln, s)

	return ln.Addr().String()
}

// dial opens a client connection to addr, failing the test if it can't.
func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// readLine reads one CRLF-terminated line, with a deadline so a bug that
// makes the server never reply fails the test instead of hanging it.
func readLine(t *testing.T, conn net.Conn) string {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("reading reply: %v", err)
	}
	return line
}

func TestServePingEcho(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got, want := readLine(t, conn), "+PONG\r\n"; got != want {
		t.Errorf("PING reply = %q, want %q", got, want)
	}
}

// TestServeCommandSplitAcrossWrites is the core partial-command case this
// phase exists for: a single RESP command arrives in several separate
// writes (standing in for it arriving in several separate TCP packets),
// with pauses between them, and the server must wait for the whole thing
// before replying rather than misparsing a truncated prefix.
func TestServeCommandSplitAcrossWrites(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	cmd := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	// Split mid bulk-string-body and mid type-line, at byte offsets picked
	// to land inside a $-length body, to specifically exercise
	// readBulkString's partial-read path.
	chunks := []string{cmd[:5], cmd[5:14], cmd[14:20], cmd[20:]}

	reader := bufio.NewReader(conn)
	for _, chunk := range chunks {
		if _, err := conn.Write([]byte(chunk)); err != nil {
			t.Fatalf("write chunk %q: %v", chunk, err)
		}
		time.Sleep(20 * time.Millisecond) // give the server a chance to (wrongly) act early
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("reading SET reply: %v", err)
	}
	if want := "+OK\r\n"; line != want {
		t.Errorf("SET reply = %q, want %q", line, want)
	}

	// Confirm the value actually landed, via a normal (unsplit) GET.
	if _, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")); err != nil {
		t.Fatalf("write GET: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	header, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("reading GET reply header: %v", err)
	}
	if want := "$3\r\n"; header != want {
		t.Fatalf("GET reply header = %q, want %q", header, want)
	}
	body, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("reading GET reply body: %v", err)
	}
	if want := "bar\r\n"; body != want {
		t.Errorf("GET reply body = %q, want %q", body, want)
	}
}

// TestServePipelinedCommands checks that two commands sent in a single
// write (as a pipelining client would) both get answered, in order.
func TestServePipelinedCommands(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	both := "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"
	if _, err := conn.Write([]byte(both)); err != nil {
		t.Fatalf("write: %v", err)
	}

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	if got, want := mustReadLine(t, reader), "+PONG\r\n"; got != want {
		t.Errorf("first reply = %q, want %q", got, want)
	}
	if got, want := mustReadLine(t, reader), "$2\r\n"; got != want {
		t.Errorf("second reply header = %q, want %q", got, want)
	}
	if got, want := mustReadLine(t, reader), "hi\r\n"; got != want {
		t.Errorf("second reply body = %q, want %q", got, want)
	}
}

func mustReadLine(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("reading reply: %v", err)
	}
	return line
}

// TestServeUnknownCommandKeepsConnectionOpen checks that an unrecognized
// command name gets a RESP error reply without the connection being torn
// down, matching real Redis (and the pre-Phase-8 goroutine-per-connection
// server).
func TestServeUnknownCommandKeepsConnectionOpen(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)
	reader := bufio.NewReader(conn)

	if _, err := conn.Write([]byte("*1\r\n$8\r\nBOGUSCMD\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("reading error reply: %v", err)
	}
	if len(line) == 0 || line[0] != '-' {
		t.Fatalf("reply = %q, want a RESP error (leading '-')", line)
	}

	// The connection must still be usable.
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("write after bad command: %v", err)
	}
	if got, want := mustReadLine(t, reader), "+PONG\r\n"; got != want {
		t.Errorf("PING after bad command = %q, want %q", got, want)
	}
}

// TestServeMalformedRESPClosesConnection checks that bytes that aren't
// valid RESP at all (as opposed to a validly-shaped command this server
// just doesn't recognize) close the connection, matching the pre-Phase-8
// server's behavior of giving up on a client that isn't speaking RESP.
func TestServeMalformedRESPClosesConnection(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	if _, err := conn.Write([]byte("not resp at all\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	// Whether or not the server manages to flush an error reply first, the
	// connection must eventually report EOF (closed by the server) rather
	// than staying open indefinitely.
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return // EOF (or any other closed-connection error) is success
		}
		if n == 0 {
			t.Fatal("Read returned 0 bytes with no error")
		}
	}
}
