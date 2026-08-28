# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project intent

This repository is a Redis clone built from scratch in Go — a Redis-compatible in-memory
data store. Nothing has been implemented yet; this file describes where the project is
headed so future work stays consistent. Planned/expected components:

- **RESP protocol** — a server that speaks the Redis Serialization Protocol so real Redis
  clients (redis-cli, standard client libraries) can talk to it.
- **Event loop** — connection handling built on epoll rather than a goroutine-per-connection
  model.
- **Core data structures** — including a custom skip list implementation backing sorted
  sets (ZSET-style commands).
- **TTL / expiry** — key expiration semantics matching Redis behavior.
- **Persistence** — RDB-style snapshotting to disk.
- **Docker packaging** — a Dockerfile/image so the server can be built and run in a container.

**Status:** Phase 1 (project scaffold + RESP protocol + TCP server with PING/ECHO),
Phase 2 (in-memory key-value store + string commands: SET, GET, DEL, EXISTS, INCR, DECR,
APPEND, STRLEN), Phase 3 (typed store + hash/list commands: HSET, HGET, HDEL, HGETALL,
HEXISTS, HLEN, LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, with WRONGTYPE errors), Phase 4
(custom skip list + sorted-set commands: ZADD, ZSCORE, ZRANK, ZRANGE, ZREM, ZINCRBY),
Phase 5 (TTL/expiry: EXPIRE, TTL, PERSIST, SET ... EX/PX, lazy + active expiry), Phase 6
(RDB-style persistence: SAVE, BGSAVE, periodic background snapshotting, reload on
startup), Phase 7 (Docker packaging: multi-stage Dockerfile, docker-compose.yml with
a persistent RDB volume), and Phase 8 (epoll-based event loop: a single-threaded,
non-blocking `internal/eventloop` implementation on Linux, with the original
goroutine-per-connection model kept as the portable fallback on every other OS via
build tags) are done. See Architecture below for what exists now. Don't assume later
phases exist until their own commits land — check the working tree and update
Architecture/Commands as each phase is completed.

## Working conventions

- **Go, idiomatic and well-commented.** Follow standard Go conventions (gofmt, effective-Go
  style). Comment non-obvious logic, especially around the epoll event loop, RESP parsing,
  and the skip list — these are the parts a future reader most needs explained.
- **Tests alongside each feature.** When a feature (a command, a data structure, a protocol
  piece) is implemented, its tests land in the same change, not as a follow-up.
- **Build incrementally, phase by phase.** Implement in small, self-contained phases (e.g.
  "basic RESP parsing + PING", "GET/SET with in-memory map", "TTL support", "skip list +
  ZSET commands", "epoll event loop", "RDB snapshotting", "Dockerfile") rather than large
  multi-feature changes.
- **Commit after each working phase.** Once a phase builds, passes its tests, and works,
  make a small, clean commit for it and push to `origin` (already configured, pointing at
  `github.com/Ishan819/Redis-clone`). Keep commits scoped to one phase's worth of work —
  avoid bundling unrelated phases into one commit.

## Commands

- **Run the server:** `go run ./cmd/redis-server` (listens on `:6379`). Test it with
  `redis-cli -p 6379 ping` / `redis-cli -p 6379 echo hi` / `redis-cli -p 6379 set k v` /
  `redis-cli -p 6379 get k` / `redis-cli -p 6379 incr counter` / `redis-cli -p 6379 hset h
  f v` / `redis-cli -p 6379 hgetall h` / `redis-cli -p 6379 rpush l a b c` /
  `redis-cli -p 6379 lrange l 0 -1` / `redis-cli -p 6379 zadd z 1 a 2 b` /
  `redis-cli -p 6379 zrange z 0 -1` / `redis-cli -p 6379 set k v EX 10` /
  `redis-cli -p 6379 ttl k` / `redis-cli -p 6379 save` (writes `dump.rdb` in the working
  directory; restart the server from the same directory to confirm data reloads), or raw
  RESP over `nc 127.0.0.1 6379` if `redis-cli` isn't installed.
- **Build:** `go build ./...`
- **Test everything:** `go test ./...` (add `-v` for per-test output)
- **Test one package:** `go test ./internal/resp/...`
- **Test one test:** `go test ./internal/resp/... -run TestReaderRead -v`
- **Vet:** `go vet ./...`
- **Format:** `gofmt -l .` to list unformatted files, `gofmt -w .` to fix
- **Cross-check the Linux epoll path from macOS:** `internal/eventloop`'s epoll
  implementation (`epoll_linux.go`) is behind a `linux` build tag, so building/testing on
  macOS only ever exercises the goroutine-per-connection fallback
  (`goroutine_other.go`). `GOOS=linux GOARCH=amd64 go build ./...` / `go vet ./...`
  compile-check the epoll file, but to actually *run* it: `docker build --target builder
  -t redis-clone-test-builder .` builds the project with the real Linux Go toolchain, then
  `docker run --rm redis-clone-test-builder sh -c 'cd /src && go vet ./... && go test
  ./...'` runs the full suite (including `internal/eventloop`'s tests) against the real
  epoll implementation on Linux. `docker rmi redis-clone-test-builder` cleans up the
  throwaway image afterward.
- **Run in Docker (one-off container):**
  `docker build -t redis-clone .` to build the image, then
  `docker run --rm -p 6379:6379 -v redis-clone-data:/data redis-clone` to run it with a
  named volume for `dump.rdb` (drop `-v ...` for a throwaway, non-persistent container).
  Test the same way as a local server: `redis-cli -p 6379 set k v` / `redis-cli -p 6379
  get k`.
- **Run in Docker (docker compose, preferred for local dev):** `docker compose up -d
  --build` to build and start; `docker compose logs -f` to follow logs; `docker compose
  down` to stop and remove the container (the named volume `rdb-data`, and the `dump.rdb`
  snapshot inside it, survive `down`/`up` — only `docker compose down -v` deletes it too).

## Architecture

Module: `github.com/Ishan819/Redis-clone`.

- `cmd/redis-server/main.go` — entrypoint; constructs a `server.Server` and calls
  `ListenAndServe`.
- `internal/resp/` — RESP2 encoding/decoding, independent of networking or commands.
  `Value` is a tagged union (`Type` + `Str`/`Num`/`Array`/`Null` fields) covering simple
  strings, errors, integers, bulk strings, and arrays. `Value.Marshal()` encodes to wire
  bytes; `Reader` (wraps a `bufio.Reader`) decodes a stream of values one at a time via
  `Read()`, recursing into `Read()` for array elements; `Reader.Buffered()` exposes the
  underlying `bufio.Reader`'s buffered-byte count, letting a caller that fed `Read` a
  byte slice (rather than a live connection) work out exactly how many bytes of that
  slice one `Read()` call consumed — `internal/eventloop`'s epoll implementation uses
  this to advance its own per-connection buffer past a decoded command. This package
  knows nothing about Redis commands or networking — it's pure protocol.
- `internal/store/` — the in-memory key-value store, independent of RESP and commands.
  `Store` wraps a `map[string]entry` guarded by a `sync.RWMutex` (one mutex for the whole
  map — fine at this scale). Each key holds a typed `entry` (`kind` — string, hash, list,
  or zset — plus whichever of `str`/`hash`/`list`/`zset` applies, plus an `expireAt
  time.Time`), so every accessor checks the key's kind and fails with `errWrongType`
  ("WRONGTYPE Operation against a key holding the wrong kind of value") if it doesn't
  match, matching Redis's cross-type behavior. A hash, list, or zset that's emptied by a
  delete/pop/rem removes the key entirely, matching Redis (an empty collection doesn't
  exist). String methods (`Set`, `Get`, `Del`, `Exists`, `IncrBy`/`Incr`/`Decr`, `Append`,
  `Strlen`) take and return plain Go values; `IncrBy`/`Append` preserve any existing TTL
  (they modify a value in place, unlike `Set`) and `IncrBy` does its read-modify-write
  under a single lock so concurrent INCRs are atomic; all return a plain Go `error`
  (message text already Redis-error-shaped) on a non-integer value, int64 overflow, or
  wrong type. Hash methods (`HSet`, `HGet`, `HDel`, `HGetAll`, `HExists`, `HLen`) operate
  on a `map[string]string` per key; `HGetAll` returns a copy, not a live view. List
  methods (`LPush`, `RPush`, `LPop`, `RPop`, `LRange`, `LLen`) operate on a `[]string` per
  key; `LRange` implements Redis's negative-index semantics (-1 is the last element) and
  clamps out-of-range indices to an empty result rather than erroring. Sorted-set methods
  (`ZAdd`, `ZScore`, `ZRank`, `ZRange`, `ZRem`, `ZIncrBy`) operate on a `*zsetValue` per
  key — a `skiplist.SkipList` (ordering) paired with a `map[string]float64` (O(1) member
  -> score lookup for `ZScore`), the same two-structure design Redis's own sorted sets
  use; `ZAdd`/`ZIncrBy` keep both in sync by deleting a member's old skip list entry
  before reinserting it at its new score.
  **Expiry** (`Expire`, `TTLSeconds`, `Persist`, `SetEx`, `StartActiveExpiry`) has two
  halves, matching Redis: lazy — every accessor routes key lookups through the internal
  `lookup` helper, which treats a key past its `expireAt` as absent (deleting it from the
  map when the caller holds the write lock, since a read-lock caller can't safely mutate
  the map); and active — `StartActiveExpiry(interval)` runs a background goroutine
  (started by `internal/server` at 100ms, matching Redis's own default active-expire
  cadence) that each tick samples up to 20 keys from `keysWithTTL` (a side map of just the
  keys with a TTL set, so the sweep doesn't have to scan the whole keyspace) and deletes
  any that have expired, reclaiming memory even for keys nothing ever reads again.
  `setEntry`/`deleteKey` are the only places allowed to write `s.data` directly — every
  other method goes through them so `keysWithTTL` never drifts out of sync. `Set`
  (`SetEx` with no TTL) clears any existing TTL, matching Redis's default `SET` behavior;
  `SetEx(key, value, ttl)` is the `SET ... EX/PX` path.
  **Persistence** (`internal/store/persistence.go`) is RDB-*style*, not RDB-*compatible*:
  `Snapshot()` walks the live (non-expired) keys under a read lock and builds a
  gob-serializable `snapshot` (a flat `[]snapshotKey`, every field exported so
  `encoding/gob`'s reflection can see it; a zset is flattened to its member/score pairs
  rather than serializing the skiplist, since the skiplist is cheap to rebuild via
  repeated `Insert` on load). `SaveToFile(path)` gob-encodes that snapshot to a temp file
  in the same directory and renames it into place, so a crash or concurrent read mid-write
  can never observe a truncated file. `LoadFromFile(path)` decodes and calls `Restore`,
  which replaces the store's entire contents and rebuilds each zset's skiplist from its
  member/score list; a missing file returns an error wrapping `os.ErrNotExist` so callers
  can tell "no snapshot yet" apart from a real failure. `DefaultRDBPath` ("dump.rdb",
  matching Redis's own default dbfilename) is the single shared path constant used by the
  command layer (SAVE/BGSAVE) and the server (startup load, periodic snapshot) so they
  never drift onto different files.
- `internal/skiplist/` — a skip list built from scratch (no libraries), independent of the
  store and RESP. `SkipList` stores `Element{Score, Member}` pairs ordered by `Score`
  ascending with ties broken by `Member` ascending, matching Redis sorted-set ordering.
  Each node's forward pointers carry a `span` (level-0 hop count to the next node at that
  level), the same technique Redis's own `t_zset.c` uses, giving `Insert`, `Delete`, and
  `Rank` expected O(log n) and `Range` (by rank, with Redis-style negative-index clamping)
  expected O(log n + k). `SkipList` is a raw ordered container — it doesn't itself enforce
  one entry per member, so `store`'s zset methods delete a member's old entry before
  inserting its new score. Not safe for concurrent use on its own; the store's mutex
  guards it. Thoroughly unit tested, including a randomized test that drives thousands of
  random inserts/deletes against a plain sorted-slice reference model and cross-checks
  `Range`, `Rank`, and `Len` after every operation.
- `internal/command/` — the command registry. `Lookup(name string) Handler` case-insensitively
  maps a command name to a `Handler func(s *store.Store, args []string) resp.Value`. Adding
  a new command means writing a handler function and adding it to the `registry` map —
  nothing outside this package needs to change. Handlers take the shared store plus
  already-decoded string args (not RESP `Value`s) and return the `resp.Value` reply
  directly, translating store errors (including WRONGTYPE) into RESP error replies.
  Implemented so far: PING, ECHO; strings — SET (with optional `EX seconds`/`PX
  milliseconds`), GET, DEL, EXISTS, INCR, DECR, APPEND, STRLEN; hashes — HSET, HGET,
  HDEL, HGETALL, HEXISTS, HLEN; lists — LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN; sorted
  sets — ZADD, ZSCORE, ZRANK, ZRANGE, ZREM, ZINCRBY (scores are formatted with
  `strconv.FormatFloat(score, 'f', -1, 64)`, the shortest round-tripping decimal — not
  byte-for-byte identical to Redis's own dtoa for exotic values, but matches for all
  typical scores); expiry — EXPIRE, TTL, PERSIST; persistence — SAVE (synchronous,
  blocks the client until `store.SaveToFile` returns) and BGSAVE (starts the same save in
  a goroutine and replies immediately — this project has no `fork()`, so "background"
  means concurrent-with-serving rather than a forked child process as real Redis uses;
  the store's own locking makes that safe).
- `internal/server/` — the TCP front end's lifecycle owner. `Server` owns one
  `*store.Store`, shared by all connections. `ListenAndServe` binds the listener, loads
  `store.DefaultRDBPath` into the store if it exists (logging either way — a missing file
  is a normal first run, not an error), starts the active-expiry background sweep (100ms
  interval), starts an unconditional periodic snapshot (`startPeriodicSnapshot`, 60s
  interval — simpler than Redis's change-triggered save points, at the cost of
  occasionally saving an unchanged dataset), and then hands the listener and store off to
  `eventloop.Serve`, which owns the actual accept/read/dispatch loop (see
  `internal/eventloop` below) — `ListenAndServe` doesn't know or care which of that
  package's two build-tagged implementations it's running.
- `internal/eventloop/` — the accept/read/dispatch loop, decoding RESP off each
  connection and invoking `command.Lookup` + `Handler` against the shared store, same as
  every phase before it. As of Phase 8 it has two implementations, chosen at compile time
  by build tag on GOOS, both exporting the same `Serve(ln net.Listener, s *store.Store)
  error`:
  - `epoll_linux.go` (`linux`) — a single-threaded, non-blocking event loop built
    directly on the epoll(7) syscalls. One goroutine owns one `epoll_create1` instance
    and every connection's raw fd (pulled out of `net.Listener`/accepted connections via
    `syscall.Conn`/`Accept4`, bypassing Go's own runtime netpoller so there's exactly one
    readiness mechanism in play, not two stacked on each other); `epoll_wait` readiness
    (`EPOLLIN`/`EPOLLOUT`, level-triggered) drives non-blocking `Read`/`Write` syscalls,
    so the loop goroutine never blocks on a slow or idle client. Each connection carries
    its own `readBuf`/`writeBuf` (`bytes.Buffer`): a RESP command split across multiple
    TCP packets is handled by accumulating bytes into `readBuf` across `EPOLLIN` events
    and attempting a decode (via the same `resp.Reader`, wrapped around the buffered
    bytes) after each one — a decode that fails with `io.EOF`/`io.ErrUnexpectedEOF` means
    "incomplete, wait for more," and a successful decode uses the new `resp.Reader.
    Buffered()` to find exactly how many bytes it consumed so `readBuf` can advance past
    it and try again (pipelined commands in one packet are drained the same way). Replies
    queue in `writeBuf` and flush opportunistically; `EPOLLOUT` is registered only while a
    write would otherwise block and dropped again once `writeBuf` drains, since a
    level-triggered "always writable" fd would otherwise re-fire every `epoll_wait` for no
    reason.
  - `goroutine_other.go` (`!linux`) — the original goroutine-per-connection model
    (Phases 1-7's `Server.handleConn`, moved here unchanged): one goroutine per
    connection, blocking reads via `resp.Reader` directly on the `net.Conn`. This is the
    portable fallback for every OS besides Linux — this project is developed on macOS,
    and epoll is Linux-only (macOS/BSD's equivalent, kqueue, isn't implemented here).
  - `eventloop.go` (no build tag) holds what both implementations share: `toArgs`
    (converts a decoded RESP value into a command name + args, requiring a RESP array of
    bulk strings) and the RESP-error-reply helpers, so the two implementations'
    command-dispatch behavior — unknown commands and malformed requests get a RESP error
    reply rather than closing the connection; a genuine RESP decode error closes the
    connection — is shared code, not just a parallel claim in two files.
    `eventloop_test.go` (also no build tag) runs the same black-box test suite (partial
    commands split across writes, pipelining, unknown commands, malformed input) over a
    real loopback connection against whichever implementation the build tag selected, so
    "the two are behaviorally equivalent" is a tested property, not just a comment —
    though since epoll is Linux-only, actually exercising `epoll_linux.go`'s tests
    requires running on/in Linux (verified via `docker build --target builder` + `docker
    run ... go test ./...`, see Commands above), not just macOS's `go test ./...`.
- `Dockerfile` — multi-stage build. Stage 1 (`golang:1.27-alpine`, matching the Go version
  in `go.mod`) compiles a static binary (`CGO_ENABLED=0`) so the runtime stage needs no
  libc/shared libraries. Stage 2 (`alpine:3.20`, chosen over a distroless/scratch base
  specifically so it's easy to `docker exec` in and inspect `dump.rdb` or debug a volume
  issue) copies in just that binary, sets `WORKDIR /data` (so the server's relative
  `store.DefaultRDBPath` reads/writes there), and `EXPOSE`s 6379.
- `docker-compose.yml` — builds the image from the local `Dockerfile`, maps host port 6379
  to the container, and mounts a named volume (`rdb-data`) at `/data` so the RDB snapshot
  outlives `docker compose down`/`up` and container recreation, not just a plain restart —
  verified by populating data, `docker compose down`, `docker compose up`, and confirming
  the data was still there.
- `.dockerignore` — keeps the build context (and thus the image) free of `.git`, local
  build artifacts, and any stray `*.rdb` file from local testing.

**Data flow:** raw bytes on a connection → `resp.Reader.Read()` (fed either directly by a
`net.Conn`, in the `!linux` fallback, or by a per-connection buffer accumulated across
`epoll` readiness events, in the `linux` implementation) → `[]string` args
(`eventloop.toArgs`) → `command.Lookup` + `Handler(store, args)` → `resp.Value` reply →
`Value.Marshal()` → written back on the same connection. `internal/resp` and
`internal/command` are unchanged by Phase 8's event loop rewrite, exactly as planned —
neither package depends on how bytes arrive on the connection.
