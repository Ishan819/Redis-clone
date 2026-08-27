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
startup), and Phase 7 (Docker packaging: multi-stage Dockerfile, docker-compose.yml with
a persistent RDB volume) are done. See Architecture below for what exists now. Don't
assume later phases (event loop) exist until their own commits land — check the working
tree and update Architecture/Commands as each phase is completed.

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
  `Read()`, recursing into `Read()` for array elements. This package knows nothing about
  Redis commands — it's pure protocol.
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
- `internal/server/` — the TCP front end, currently one goroutine per connection (`Server.handleConn`).
  `Server` owns one `*store.Store`, shared by all connections. `ListenAndServe` loads
  `store.DefaultRDBPath` into the store if it exists (logging either way — a missing file
  is a normal first run, not an error), starts the active-expiry background sweep (100ms
  interval), and starts an unconditional periodic snapshot (`startPeriodicSnapshot`,
  60s interval — simpler than Redis's change-triggered save points, at the cost of
  occasionally saving an unchanged dataset), stopping both on shutdown. Per connection:
  wrap the `net.Conn` in a `resp.Reader`, loop reading a `Value`, convert it to
  `[]string` args via `toArgs` (which enforces that a command is a RESP array of bulk
  strings), look up the `command.Handler` and invoke it with the server's store, and
  write the reply's `Marshal()` bytes back on the same connection. Unknown commands and
  malformed requests get a RESP error reply rather than closing the connection.
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

**Data flow:** `net.Conn` → `resp.Reader.Read()` → `[]string` args (`server.toArgs`) →
`command.Lookup` + `Handler(store, args)` → `resp.Value` reply → `Value.Marshal()` →
`conn.Write`.

**Known future seam:** `internal/server`'s accept/read loop is expected to be replaced by an
epoll-based event loop in a later phase; `internal/resp` and `internal/command` are designed
to be reusable as-is when that happens, since they don't depend on how bytes arrive on the
connection.
