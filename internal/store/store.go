// Package store implements the in-memory key-value store backing the
// server's commands (SET, GET, HSET, LPUSH, ...). It knows nothing about
// RESP or the command layer — callers pass and receive plain Go values,
// and errors are returned as plain errors whose message text is already
// Redis-error-shaped (e.g. "ERR value is not an integer or out of range",
// "WRONGTYPE Operation against a key holding the wrong kind of value") so
// the command layer can wrap them directly in a resp.Value.
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

// errWrongType is returned whenever a command is used against a key that
// holds a value of a different type, matching Redis's WRONGTYPE error
// (e.g. calling LPUSH on a key that holds a string).
var errWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// kind identifies which of the store's supported types a key holds.
type kind int

const (
	kindString kind = iota
	kindHash
	kindList
)

// entry is the store's internal representation of one key's value. Only
// the field matching kind is populated.
type entry struct {
	kind kind
	str  string
	hash map[string]string
	list []string
}

// Store is a thread-safe in-memory key-value store supporting strings,
// hashes, and lists. The zero value is not usable; construct one with New.
// A single mutex guards the whole map for now — fine at this scale, and
// simple to reason about while later phases (TTL, more data types) still
// land on top of it.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry
}

// New returns an empty, ready-to-use Store.
func New() *Store {
	return &Store{data: make(map[string]entry)}
}

// --- Strings ---

// Set stores value as a string under key, overwriting any existing value
// regardless of its type — matching Redis, where SET always replaces
// whatever was there.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{kind: kindString, str: value}
}

// Get returns the string stored at key and whether it was present. It
// fails with errWrongType if key holds a hash or list.
func (s *Store) Get(key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return "", false, nil
	}
	if e.kind != kindString {
		return "", false, errWrongType
	}
	return e.str, true, nil
}

// Del removes each of keys, if present, regardless of type, and returns
// how many were actually deleted (Redis's DEL return value).
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

// Exists returns how many of keys are currently present, regardless of
// type. Redis counts a key multiple times if it's repeated in the
// argument list, so this does too.
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
// a missing key as 0) and stores + returns the result. It fails if key
// holds a hash or list, if the existing string value isn't a base-10
// int64, or if the addition would overflow int64 — all matching Redis's
// INCR/DECR/INCRBY/DECRBY behavior.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var cur int64
	if e, ok := s.data[key]; ok {
		if e.kind != kindString {
			return 0, errWrongType
		}
		n, err := strconv.ParseInt(e.str, 10, 64)
		if err != nil {
			return 0, errNotInteger
		}
		cur = n
	}

	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, fmt.Errorf("ERR increment or decrement would overflow")
	}

	next := cur + delta
	s.data[key] = entry{kind: kindString, str: strconv.FormatInt(next, 10)}
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

// Append appends value to the string stored at key (treating a missing key
// as the empty string) and returns the length of the result, matching
// Redis's APPEND return value. It fails with errWrongType if key holds a
// hash or list.
func (s *Store) Append(key, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if ok && e.kind != kindString {
		return 0, errWrongType
	}
	next := e.str + value
	s.data[key] = entry{kind: kindString, str: next}
	return len(next), nil
}

// Strlen returns the length of the string stored at key, or 0 if key
// doesn't exist, matching Redis's STRLEN. It fails with errWrongType if
// key holds a hash or list.
func (s *Store) Strlen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return 0, nil
	}
	if e.kind != kindString {
		return 0, errWrongType
	}
	return len(e.str), nil
}

// --- Hashes ---

// HSet sets fields to values in the hash at key, creating the hash if it
// doesn't exist. pairs must be an even-length, flat field/value sequence
// (field1, value1, field2, value2, ...) — callers validate arity before
// calling. It returns the number of fields that were newly created (not
// merely overwritten), matching Redis's HSET return value, and fails with
// errWrongType if key holds a string or list.
func (s *Store) HSet(key string, pairs ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if ok && e.kind != kindHash {
		return 0, errWrongType
	}
	if !ok {
		e = entry{kind: kindHash, hash: make(map[string]string)}
	}

	added := 0
	for i := 0; i+1 < len(pairs); i += 2 {
		field, value := pairs[i], pairs[i+1]
		if _, exists := e.hash[field]; !exists {
			added++
		}
		e.hash[field] = value
	}
	s.data[key] = e
	return added, nil
}

// HGet returns the value of field in the hash at key, and whether it was
// present. A missing key behaves like an empty hash (ok=false, no error).
// It fails with errWrongType if key holds a string or list.
func (s *Store) HGet(key, field string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return "", false, nil
	}
	if e.kind != kindHash {
		return "", false, errWrongType
	}
	v, ok := e.hash[field]
	return v, ok, nil
}

// HDel removes each of fields from the hash at key and returns how many
// were actually removed. If the hash becomes empty, the key itself is
// removed, matching Redis (a hash with no fields doesn't exist). It fails
// with errWrongType if key holds a string or list.
func (s *Store) HDel(key string, fields ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		return 0, nil
	}
	if e.kind != kindHash {
		return 0, errWrongType
	}

	count := 0
	for _, f := range fields {
		if _, exists := e.hash[f]; exists {
			delete(e.hash, f)
			count++
		}
	}
	if len(e.hash) == 0 {
		delete(s.data, key)
	} else {
		s.data[key] = e
	}
	return count, nil
}

// HGetAll returns a copy of all field/value pairs in the hash at key, or
// an empty map if key doesn't exist. It fails with errWrongType if key
// holds a string or list.
func (s *Store) HGetAll(key string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return map[string]string{}, nil
	}
	if e.kind != kindHash {
		return nil, errWrongType
	}
	result := make(map[string]string, len(e.hash))
	for k, v := range e.hash {
		result[k] = v
	}
	return result, nil
}

// HExists reports whether field is present in the hash at key. It fails
// with errWrongType if key holds a string or list.
func (s *Store) HExists(key, field string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return false, nil
	}
	if e.kind != kindHash {
		return false, errWrongType
	}
	_, exists := e.hash[field]
	return exists, nil
}

// HLen returns the number of fields in the hash at key, or 0 if key
// doesn't exist. It fails with errWrongType if key holds a string or list.
func (s *Store) HLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return 0, nil
	}
	if e.kind != kindHash {
		return 0, errWrongType
	}
	return len(e.hash), nil
}

// --- Lists ---

// LPush pushes each of values onto the head (left) of the list at key, one
// at a time — so for LPush(key, "a", "b"), "b" ends up before "a" — and
// returns the list's new length, matching Redis's LPUSH. It creates the
// list if key doesn't exist, and fails with errWrongType if key holds a
// string or hash.
func (s *Store) LPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if ok && e.kind != kindList {
		return 0, errWrongType
	}
	if !ok {
		e = entry{kind: kindList}
	}
	for _, v := range values {
		e.list = append([]string{v}, e.list...)
	}
	s.data[key] = e
	return len(e.list), nil
}

// RPush pushes each of values onto the tail (right) of the list at key, in
// order, and returns the list's new length, matching Redis's RPUSH. It
// creates the list if key doesn't exist, and fails with errWrongType if
// key holds a string or hash.
func (s *Store) RPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if ok && e.kind != kindList {
		return 0, errWrongType
	}
	if !ok {
		e = entry{kind: kindList}
	}
	e.list = append(e.list, values...)
	s.data[key] = e
	return len(e.list), nil
}

// LPop removes and returns the first element of the list at key. ok is
// false if key doesn't exist or the list is empty (Redis returns nil in
// both cases). If the list becomes empty, the key itself is removed. It
// fails with errWrongType if key holds a string or hash.
func (s *Store) LPop(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		return "", false, nil
	}
	if e.kind != kindList {
		return "", false, errWrongType
	}
	if len(e.list) == 0 {
		delete(s.data, key)
		return "", false, nil
	}
	v := e.list[0]
	e.list = e.list[1:]
	if len(e.list) == 0 {
		delete(s.data, key)
	} else {
		s.data[key] = e
	}
	return v, true, nil
}

// RPop removes and returns the last element of the list at key. ok is
// false if key doesn't exist or the list is empty (Redis returns nil in
// both cases). If the list becomes empty, the key itself is removed. It
// fails with errWrongType if key holds a string or hash.
func (s *Store) RPop(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		return "", false, nil
	}
	if e.kind != kindList {
		return "", false, errWrongType
	}
	if len(e.list) == 0 {
		delete(s.data, key)
		return "", false, nil
	}
	last := len(e.list) - 1
	v := e.list[last]
	e.list = e.list[:last]
	if len(e.list) == 0 {
		delete(s.data, key)
	} else {
		s.data[key] = e
	}
	return v, true, nil
}

// LRange returns the elements of the list at key between start and stop,
// inclusive, matching Redis's LRANGE index semantics: negative indices
// count from the end of the list (-1 is the last element), and an
// out-of-range or empty result yields an empty (non-nil) slice rather than
// an error. A missing key behaves like an empty list. It fails with
// errWrongType if key holds a string or hash.
func (s *Store) LRange(key string, start, stop int64) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return []string{}, nil
	}
	if e.kind != kindList {
		return nil, errWrongType
	}

	n := int64(len(e.list))
	if n == 0 {
		return []string{}, nil
	}
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if start >= n || start > stop {
		return []string{}, nil
	}
	if stop >= n {
		stop = n - 1
	}

	result := make([]string, stop-start+1)
	copy(result, e.list[start:stop+1])
	return result, nil
}

// LLen returns the length of the list at key, or 0 if key doesn't exist.
// It fails with errWrongType if key holds a string or hash.
func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return 0, nil
	}
	if e.kind != kindList {
		return 0, errWrongType
	}
	return len(e.list), nil
}
