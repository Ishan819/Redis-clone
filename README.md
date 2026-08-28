# Redis Clone

A Redis-compatible in-memory data store, built from scratch in Go — no
third-party dependencies (`go.mod` has zero `require` lines). It speaks
real RESP over TCP, so `redis-cli` and standard Redis client libraries work
against it unmodified.

This is a from-scratch learning project, built phase by phase (see git
history / commit messages: `Phase 1` through `Phase 9`). It implements a
useful subset of Redis — strings, hashes, lists, sorted sets, TTL,
persistence, an epoll event loop — not the full command set or full RESP3
protocol.

## Architecture

```
net.Conn/epoll fd → resp.Reader.Read() → []string args (eventloop.toArgs)
  → command.Lookup + Handler(store, args) → resp.Value → Value.Marshal()
  → written back on the connection
```

- **`internal/resp`** — RESP2 encoding/decoding. `Value` is a tagged union
  covering simple strings, errors, integers, bulk strings, and arrays.
  `Reader` decodes a stream of values off any `io.Reader`, one at a time,
  including partial framing (see the event loop section below). This
  package knows nothing about Redis commands or how bytes arrive on the
  wire — it's pure protocol.

- **`internal/store`** — the in-memory key-value store. One
  `map[string]entry` guarded by a single `sync.RWMutex`. Each key holds a
  *typed* entry (string, hash, list, or zset); every accessor checks the
  key's kind first and returns a `WRONGTYPE` error if it doesn't match,
  matching real Redis's cross-type behavior. An emptied hash/list/zset
  (last field deleted, last element popped) removes the key entirely — an
  empty collection doesn't exist, same as Redis.

  - **Strings**: `Set`, `Get`, `Del`, `Exists`, `IncrBy`/`Incr`/`Decr`,
    `Append`, `Strlen`. `IncrBy` does its read-modify-write under one lock
    so concurrent `INCR`s are atomic.
  - **Hashes**: `HSet`, `HGet`, `HDel`, `HGetAll`, `HExists`, `HLen` over a
    `map[string]string` per key.
  - **Lists**: `LPush`, `RPush`, `LPop`, `RPop`, `LRange`, `LLen` over a
    `[]string` per key, with Redis-style negative-index semantics
    (`-1` = last element) and out-of-range indices clamped to an empty
    result rather than erroring.
  - **Sorted sets**: `ZAdd`, `ZScore`, `ZRank`, `ZRange`, `ZRem`,
    `ZIncrBy`, backed by a custom skip list (see below) paired with a
    `map[string]float64` for O(1) `ZSCORE` lookups — the same
    two-structure design Redis's own `t_zset.c` uses.
  - **Expiry**: lazy (every lookup treats a key past its `expireAt` as
    absent, deleting it on the next write-locked access) plus active (a
    background sweep, started by `internal/server` at a 100ms interval —
    Redis's own default cadence — samples up to 20 keys at a time from a
    side-index of just the keys that have a TTL, so it doesn't have to
    scan the whole keyspace).
  - **Persistence**: RDB-*style*, not RDB-*compatible* — `gob`-encoded,
    not Redis's actual binary format. `Snapshot()`/`SaveToFile()` walk the
    live (non-expired) keys under a read lock and atomically write a temp
    file + rename, so a crash or concurrent read mid-write never observes
    a truncated file. `LoadFromFile()` rebuilds each zset's skip list from
    its flattened member/score pairs (cheaper than serializing the skip
    list itself).

- **`internal/skiplist`** — a skip list built from scratch, ordering
  `{Score, Member}` pairs by score ascending, ties broken by member
  ascending. Each node's forward pointers carry a *span* (level-0 hop
  count), the same technique Redis's own skip list uses, giving `Insert`,
  `Delete`, `Rank` expected O(log n) and rank-based `Range` expected
  O(log n + k). Not safe for concurrent use on its own — the store's mutex
  guards it. Thoroughly unit tested, including a randomized test that
  cross-checks thousands of random inserts/deletes against a plain
  sorted-slice reference model.

- **`internal/command`** — the command registry: `Lookup(name)` maps a
  command name (case-insensitive) to a `Handler func(*store.Store,
  []string) resp.Value`. Adding a command means writing one function and
  adding it to a map — nothing else in this package changes.

- **`internal/server`** — owns the TCP listener's lifecycle: binds the
  address, loads an RDB snapshot on startup if one exists, starts the
  active-expiry sweep and the periodic-snapshot timer, then hands the
  listener off to `internal/eventloop` for the actual connection handling.

- **`internal/eventloop`** — the accept/read/dispatch loop, with **two
  implementations selected at compile time by build tag on GOOS**:

  - **`epoll_linux.go`** (`linux`) — a single-threaded, non-blocking
    event loop built directly on the `epoll(7)` syscalls. One goroutine
    owns one `epoll_create1` instance and every connection's raw fd
    (pulled out via `syscall.Conn`, bypassing Go's own runtime netpoller
    entirely so there's exactly one readiness mechanism in play, not two
    stacked on top of each other). `epoll_wait` readiness
    (`EPOLLIN`/`EPOLLOUT`, level-triggered) drives non-blocking
    `Read`/`Write` syscalls — that one goroutine never blocks on a slow or
    idle client, which is what lets it hold many concurrent connections
    without a per-connection goroutine and its stack/scheduling overhead.
    Each connection has its own `readBuf`/`writeBuf`: a RESP command split
    across multiple TCP packets accumulates in `readBuf` across `EPOLLIN`
    events, and a decode attempt (via the same `resp.Reader`, wrapped
    around whatever's buffered so far) after every read tells you whether
    it's complete yet — an `io.EOF`/`io.ErrUnexpectedEOF` means "not yet,
    wait for more," success means advance the buffer past however many
    bytes `resp.Reader.Buffered()` says it consumed and try again (for
    pipelined commands in one packet). Replies queue in `writeBuf` and
    flush opportunistically; `EPOLLOUT` is registered only while a write
    would otherwise block, and dropped again once the backlog drains.
  - **`goroutine_other.go`** (`!linux`) — the original
    goroutine-per-connection model: one goroutine per client, blocking
    reads via `resp.Reader` directly on the `net.Conn`. This is the
    portable fallback for every OS besides Linux (this project is
    developed on macOS; epoll is Linux-only, and kqueue — macOS/BSD's
    equivalent — isn't implemented here).

  Both export the identical `Serve(ln net.Listener, s *store.Store)
  error`, and `eventloop.go` (no build tag) holds what they share —
  `toArgs`, RESP-error-reply helpers — so their command-dispatch behavior
  is actually shared code, not just a parallel claim in two files.
  `eventloop_test.go` (also no build tag) runs the same black-box test
  suite against whichever implementation the build tag picked.

## Commands

| Category | Commands |
|---|---|
| Connection | `PING [message]`, `ECHO message` |
| Strings | `SET key value [EX seconds \| PX milliseconds]`, `GET key`, `DEL key...`, `EXISTS key...`, `INCR key`, `DECR key`, `APPEND key value`, `STRLEN key` |
| Hashes | `HSET key field value [field value ...]`, `HGET key field`, `HDEL key field...`, `HGETALL key`, `HEXISTS key field`, `HLEN key` |
| Lists | `LPUSH key value...`, `RPUSH key value...`, `LPOP key`, `RPOP key`, `LRANGE key start stop`, `LLEN key` |
| Sorted sets | `ZADD key score member [score member ...]`, `ZSCORE key member`, `ZRANK key member`, `ZRANGE key start stop`, `ZREM key member...`, `ZINCRBY key increment member` |
| Expiry | `EXPIRE key seconds`, `TTL key`, `PERSIST key` |
| Persistence | `SAVE`, `BGSAVE` |

Not implemented: `SET`'s `NX`/`XX`/`KEEPTTL` flags, `MULTI`/`EXEC`,
pub/sub, `SCAN`, `SETRANGE`/`GETRANGE`, sets (`SADD`/etc.), `OBJECT`,
`CONFIG`, RESP3, replication, clustering, ACLs. Wrong-type access (e.g.
`LPUSH` on a key holding a string) returns a `WRONGTYPE` error, matching
real Redis.

## Building and running

**Requirements:** Go 1.27+ (see `go.mod`). No external dependencies to
install.

### `go run`

```sh
go run ./cmd/redis-server        # listens on :6379
redis-cli -p 6379 ping
redis-cli -p 6379 set k v
redis-cli -p 6379 get k
redis-cli -p 6379 hset h f v
redis-cli -p 6379 zadd z 1 a 2 b
redis-cli -p 6379 zrange z 0 -1
redis-cli -p 6379 set k v EX 10
redis-cli -p 6379 ttl k
redis-cli -p 6379 save           # writes dump.rdb in the working directory
```

No `redis-cli`? Raw RESP over `nc 127.0.0.1 6379` works too. On macOS
(non-Linux), this runs the goroutine-per-connection fallback; the epoll
implementation only compiles in under `GOOS=linux`.

### Docker

```sh
# one-off container
docker build -t redis-clone .
docker run --rm -p 6379:6379 -v redis-clone-data:/data redis-clone

# docker compose (preferred for local dev — named volume survives `down`/`up`)
docker compose up -d --build
docker compose logs -f
docker compose down            # add -v to also delete the dump.rdb volume
```

The Docker image is always built with `GOOS=linux`, so **this is the epoll
event loop**, not the fallback — the same build path used for the
benchmarks below.

## Tests

```sh
go build ./...
go test ./...          # -v for per-test output
go vet ./...
gofmt -l .              # gofmt -w . to fix
```

Everything above runs the `!linux` build tag on macOS, so it only exercises
`goroutine_other.go` — `epoll_linux.go` doesn't even compile in. To
actually run the epoll implementation's tests (and everything else) on
real Linux from a Mac:

```sh
GOOS=linux GOARCH=amd64 go build ./...   # cross-compile check (type-checks epoll_linux.go)
GOOS=linux GOARCH=amd64 go vet ./...

# actually run it, on real Linux, via the same Dockerfile used for the image:
docker build --target builder -t redis-clone-test-builder .
docker run --rm redis-clone-test-builder sh -c 'cd /src && go vet ./... && go test ./...'
docker rmi redis-clone-test-builder
```

All packages pass under both paths, including `internal/eventloop`'s
partial-command-across-writes, pipelining, unknown-command, and
malformed-input tests run against the real epoll syscalls.

## Benchmarks

Full methodology, raw `redis-benchmark` output, and a longer discussion of
what these numbers do and don't show live in
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md). Headline numbers, epoll
build, `SET`/`GET`, random 100k-key keyspace, 3-byte values:

| Concurrency | SET (req/s) | GET (req/s) | SET p50 / p99 | GET p50 / p99 |
|---|---:|---:|---:|---:|
| 50 clients | 218,818 | 213,447 | 0.119 / 0.239 ms | 0.127 / 0.303 ms |
| 500 clients | 236,072 | 236,518 | 1.071 / 1.775 ms | 1.071 / 1.831 ms |

Throughput didn't degrade going from 50 to 500 concurrent connections — if
anything it went up slightly, since a single-threaded event loop has no
per-connection lock contention to add as connection count grows. Latency
scaled up roughly proportionally with the 10x concurrency increase, which
is what queuing theory (Little's Law) predicts for a single server handling
one request at a time, not a sign of anything scaling badly. Both runs
completed with zero errors and no dropped connections.

(Benchmarked in a Linux container on Docker Desktop, not dedicated
hardware — see `benchmarks/RESULTS.md` for the caveat on what that does and
doesn't mean for the absolute numbers.)

## Design notes / tradeoffs

Deliberate simplifications made along the way, and why:

- **`BGSAVE` is a goroutine, not a `fork()`.** Real Redis forks a child
  process for background saves, relying on the OS's copy-on-write page
  sharing so the save can walk a consistent snapshot of memory without
  blocking the parent. Go doesn't give you `fork()` in any usable form, so
  `BGSAVE` here just runs the same synchronous save `SAVE` uses in a
  goroutine and returns immediately — "background" means
  concurrent-with-serving, not a forked child. The store's own
  `sync.RWMutex` is what makes that safe (a save takes a read lock and
  walks the live keyspace; that's a real, if brief, lock hold across
  however many keys exist — not free, unlike Redis's fork trick, but
  correct).
- **Periodic snapshots are unconditional and timer-based, not
  change-triggered.** Real Redis only saves when its configured "save
  points" are met (e.g. "≥1 key changed in 60s"). This project just saves
  every 60 seconds regardless of whether anything changed — simpler to
  reason about, at the cost of occasionally writing an unchanged dataset
  to disk.
- **Persistence is RDB-*style*, not RDB-*compatible*.** Snapshots are
  `gob`-encoded Go structs, not Redis's actual RDB binary format. This
  project's own server can read its own snapshots; real `redis-server`
  cannot, and there's no `redis-check-rdb`-equivalent compatibility.
- **The epoll event loop is level-triggered, not edge-triggered.**
  Edge-triggered (`EPOLLET`) can save a few syscalls under heavy load, but
  only if every readable fd gets drained to `EAGAIN` on every single
  event — miss one and that connection silently stalls forever.
  Level-triggered just re-reports a fd that wasn't fully drained on the
  next `epoll_wait`, which is more forgiving for a first epoll
  implementation. The code already drains every readable fd to `EAGAIN`
  regardless, so switching to `EPOLLET` later would be a small, low-risk
  change if profiling ever called for it.
- **One mutex for the whole keyspace**, not per-key or sharded locking.
  Simple to reason about and correct by construction; the tradeoff is that
  every command briefly serializes against every other command regardless
  of whether their keys actually overlap. The benchmark numbers above are
  with this design as-is — good enough that sharding hasn't been a problem
  worth solving yet.
- **No RESP3, no `MULTI`/`EXEC`, no pub/sub, no clustering.** This is a
  from-scratch learning project scoped to a useful, testable subset of
  Redis, not a production replacement — see "Not implemented" above.
