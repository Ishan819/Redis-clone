# Benchmark results — Phase 8 epoll event loop

Goal: confirm the epoll-based event loop (`internal/eventloop/epoll_linux.go`)
actually delivers real throughput, and that a single loop goroutine holds up
under many more concurrent connections than the 50 used for the headline
numbers.

## Method

- Server: the production image built from this repo's `Dockerfile`
  (`golang:1.27-alpine` builder, `GOOS=linux` — the same build that compiles
  in `epoll_linux.go`, no test-only variant), run with no volume (throwaway
  container, no RDB persistence involved).
- Client: `redis-benchmark` from the official `redis:7-alpine` image, run in
  a separate container on the same private Docker bridge network as the
  server (so traffic crosses a real container network stack, not loopback).
- `redis-benchmark -t set,get -r 100000 -d 3`, i.e. random keys drawn from a
  100k keyspace, 3-byte values — `-r` matters: without it every client hits
  the literal string `key:__rand_int__` instead of a random key, which
  isn't representative.
- Two concurrency levels: `-c 50 -n 200000` and `-c 500 -n 500000`.
- Host: Docker Desktop on an Apple Silicon Mac (the VM reported 10 vCPUs to
  the container via `nproc`). This is a virtualized Linux environment, not
  dedicated bare-metal hardware — treat the absolute numbers as indicative
  of this project's own headroom, not as a number comparable to published
  real-Redis benchmarks run on dedicated hardware.

Raw `redis-benchmark` output (including per-second progress and full
latency-percentile tables) is saved alongside this file: `c50.txt`,
`c500.txt`.

## Results

| Concurrency | Command | Throughput (req/s) | avg latency | p50 | p95 | p99 | max |
|---|---|---:|---:|---:|---:|---:|---:|
| 50 clients | SET | 218,818 | 0.126 ms | 0.119 ms | 0.159 ms | 0.239 ms | 0.743 ms |
| 50 clients | GET | 213,447 | 0.129 ms | 0.127 ms | 0.159 ms | 0.303 ms | 3.239 ms |
| 500 clients | SET | 236,072 | 1.098 ms | 1.071 ms | 1.199 ms | 1.775 ms | 6.935 ms |
| 500 clients | GET | 236,518 | 1.098 ms | 1.071 ms | 1.215 ms | 1.831 ms | 6.511 ms |

Both runs completed with zero errors and zero dropped/reset connections;
`docker logs` on the server showed nothing but the two startup lines the
whole time, and the container never restarted.

## Reading these numbers

- **50 → 500 clients: throughput went *up*, not down**, and stayed in the
  same ~210-240k req/s band. A single-threaded event loop has no lock
  contention or context-switch cost to add as connection count grows — it's
  just one `epoll_wait` loop iterating over whichever fds are ready — so the
  extra concurrency mostly translates into keeping that loop's non-blocking
  read/write syscalls fed back-to-back instead of idling between requests
  from any one client.
- **Latency scaled with concurrency roughly as expected from queuing, not
  from anything getting slower per-request.** p50 moved from ~0.12ms at 50
  clients to ~1.07ms at 500 clients — consistent with Little's Law: with a
  single server processing one request at a time and roughly the same
  per-request service time, 10x more concurrent in-flight requests means
  ~10x more average time spent waiting in line before the loop gets to that
  particular fd. Nothing in the design (buffers, epoll registration) is
  O(n²) in connection count, which is what this would have exposed if it
  were.
- **CPU usage while under sustained load (`docker stats`) sat at roughly
  100-130% of one core** — i.e. one core fully busy, plus a bit more from
  Go's garbage collector and the two background goroutines this project
  runs outside the event loop itself (the 100ms active-expiry sweep and the
  60s periodic-snapshot timer in `internal/server`). That the number isn't
  a hard, flat 100.00% is expected and consistent with the design being
  single-threaded specifically for the request-handling loop, not for the
  whole process.
- This is a from-scratch clone with a small command surface, so the
  benchmark is deliberately scoped to `SET`/`GET` — `redis-benchmark`'s
  default full suite exercises commands (`MSET`, `SADD`, `LPUSH` variants,
  etc.) this project doesn't implement, which would just error out rather
  than measure anything.
