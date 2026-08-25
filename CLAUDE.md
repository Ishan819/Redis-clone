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

**Status:** Phase 1 (project scaffold + RESP protocol + TCP server with PING/ECHO) is done.
See Architecture below for what exists now. Don't assume later phases (event loop, TTL,
skip list, persistence, Docker) exist until their own commits land — check the working tree
and update Architecture/Commands as each phase is completed.

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
  `redis-cli -p 6379 ping` / `redis-cli -p 6379 echo hi`, or raw RESP over `nc 127.0.0.1 6379`
  if `redis-cli` isn't installed.
- **Build:** `go build ./...`
- **Test everything:** `go test ./...` (add `-v` for per-test output)
- **Test one package:** `go test ./internal/resp/...`
- **Test one test:** `go test ./internal/resp/... -run TestReaderRead -v`
- **Vet:** `go vet ./...`
- **Format:** `gofmt -l .` to list unformatted files, `gofmt -w .` to fix

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
- `internal/command/` — the command registry. `Lookup(name string) Handler` case-insensitively
  maps a command name to a `Handler func(args []string) resp.Value`. Adding a new command
  means writing a handler function and adding it to the `registry` map — nothing outside
  this package needs to change. Handlers take already-decoded string args (not RESP `Value`s)
  and return the `resp.Value` reply directly.
- `internal/server/` — the TCP front end, currently one goroutine per connection (`Server.handleConn`).
  Per connection: wrap the `net.Conn` in a `resp.Reader`, loop reading a `Value`, convert it
  to `[]string` args via `toArgs` (which enforces that a command is a RESP array of bulk
  strings), look up and invoke the `command.Handler`, and write the reply's `Marshal()`
  bytes back on the same connection. Unknown commands and malformed requests get a RESP
  error reply rather than closing the connection.

**Data flow:** `net.Conn` → `resp.Reader.Read()` → `[]string` args (`server.toArgs`) →
`command.Lookup` + `Handler` → `resp.Value` reply → `Value.Marshal()` → `conn.Write`.

**Known future seam:** `internal/server`'s accept/read loop is expected to be replaced by an
epoll-based event loop in a later phase; `internal/resp` and `internal/command` are designed
to be reusable as-is when that happens, since they don't depend on how bytes arrive on the
connection.
