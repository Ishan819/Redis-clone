# syntax=docker/dockerfile:1

# --- Build stage -------------------------------------------------------
# Compiles the redis-clone binary in a full Go toolchain image. Nothing
# from this stage ends up in the final image except the compiled binary,
# so build tools, module cache, and source never bloat the runtime image.
FROM golang:1.27-alpine AS builder

WORKDIR /src

# Copy just the module files first so `go mod download` is cached across
# builds whenever only application code (not dependencies) changes. This
# project currently has zero third-party dependencies, but this pattern
# costs nothing and keeps the Dockerfile correct if that ever changes.
COPY go.mod ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a fully static binary, which is what lets the
# final stage be a minimal image (no libc, no shared libraries to match).
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/redis-server ./cmd/redis-server

# --- Runtime stage -------------------------------------------------------
# A minimal Alpine base rather than distroless: this project persists an
# RDB snapshot to disk (see internal/store/persistence.go), and Alpine's
# shell and coreutils make it easy to exec into the container to inspect
# dump.rdb or debug a volume-mount issue — worth the extra few MB over a
# distroless/static image for a project at this stage.
FROM alpine:3.20

WORKDIR /data
COPY --from=builder /out/redis-server /usr/local/bin/redis-server

# The server's default RDB path (store.DefaultRDBPath) is "dump.rdb",
# resolved relative to the process's working directory — so WORKDIR here
# doubles as where the snapshot is read from and written to. docker-
# compose.yml mounts a volume here so the snapshot survives container
# restarts and recreation.
EXPOSE 6379

ENTRYPOINT ["redis-server"]
