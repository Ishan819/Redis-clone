// Package store implements the in-memory key-value store backing the
// server's string commands (SET, GET, DEL, ...). It knows nothing about
// RESP or the command layer — callers pass and receive plain Go values,
// and errors are returned as plain errors whose message text is already
// Redis-error-shaped (e.g. "ERR value is not an integer or out of range")
// so the command layer can wrap them directly in a resp.Value.
package store

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
)

// errNotInteger is returned by IncrBy when the key's current value can't be
// parsed as a base-10 int64, matching Redis's error for INCR/DECR on a
// non-numeric string.
var errNotInteger = errors.New("ERR value is not an integer or out of range")

// Store is a thread-safe in-memory key-value store. The zero value is not
// usable; construct one with New. A single mutex guards the whole map for
// now — fine at this scale, and simple to reason about while later phases
// (TTL, more data types) still land on top of it.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty, ready-to-use Store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Set stores value under key, overwriting any existing value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value stored at key and whether it was present.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Del removes each of keys, if present, and returns how many were actually
// deleted (Redis's DEL return value).
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			count++
		}
	}
	return count
}

// Exists returns how many of keys are currently present. Redis counts a key
// multiple times if it's repeated in the argument list, so this does too.
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			count++
		}
	}
	return count
}

// IncrBy atomically adds delta to the integer value stored at key (treating
// a missing key as 0) and stores + returns the result. It fails if the
// existing value isn't a base-10 int64, or if the addition would overflow
// int64 — both matching Redis's INCR/DECR/INCRBY/DECRBY behavior.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var cur int64
	if v, ok := s.data[key]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, errNotInteger
		}
		cur = n
	}

	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, fmt.Errorf("ERR increment or decrement would overflow")
	}

	next := cur + delta
	s.data[key] = strconv.FormatInt(next, 10)
	return next, nil
}

// Incr is IncrBy(key, 1).
func (s *Store) Incr(key string) (int64, error) {
	return s.IncrBy(key, 1)
}

// Decr is IncrBy(key, -1).
func (s *Store) Decr(key string) (int64, error) {
	return s.IncrBy(key, -1)
}

// Append appends value to whatever is stored at key (treating a missing key
// as the empty string) and returns the length of the result, matching
// Redis's APPEND return value.
func (s *Store) Append(key, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.data[key] + value
	s.data[key] = next
	return len(next)
}

// Strlen returns the length of the value stored at key, or 0 if key doesn't
// exist, matching Redis's STRLEN.
func (s *Store) Strlen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data[key])
}
