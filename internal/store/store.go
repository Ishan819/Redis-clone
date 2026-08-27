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
	"time"

	"github.com/Ishan819/Redis-clone/internal/skiplist"
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
	kindZSet
)

// entry is the store's internal representation of one key's value. Only
// the field matching kind is populated. expireAt is the key's TTL
// deadline; the zero Time value means "no expiry", matching how a
// never-set time.Time already reads as "not a real time" via IsZero.
type entry struct {
	kind     kind
	str      string
	hash     map[string]string
	list     []string
	zset     *zsetValue
	expireAt time.Time
}

// zsetValue is a sorted set: a skiplist.SkipList giving ordered access
// (Rank, Range) plus a member -> score map giving O(1) ZSCORE lookups —
// the same two-structure design Redis's own sorted sets use, since a skip
// list alone can't answer "what's this member's score" without an O(n)
// scan.
type zsetValue struct {
	sl     *skiplist.SkipList
	scores map[string]float64
}

func newZSet() *zsetValue {
	return &zsetValue{sl: skiplist.New(), scores: make(map[string]float64)}
}

// ZMember is one (member, score) pair returned by ZRange.
type ZMember struct {
	Member string
	Score  float64
}

// Store is a thread-safe in-memory key-value store supporting strings,
// hashes, lists, and sorted sets, with optional per-key TTLs. The zero
// value is not usable; construct one with New. A single mutex guards the
// whole map for now — fine at this scale, and simple to reason about.
//
// Expiry has two halves, matching Redis:
//   - Lazy: every accessor treats an expired key as absent (via lookup
//     below), so a stale key is never visibly returned even if the active
//     sweep hasn't reclaimed it yet.
//   - Active: StartActiveExpiry runs a background goroutine that
//     periodically samples keysWithTTL and deletes anything expired, so
//     memory for expired keys is reclaimed even if nothing ever reads
//     them again.
//
// keysWithTTL tracks which keys currently have a TTL set, so the active
// sweep only has to examine those keys instead of the whole keyspace.
// setEntry and deleteKey are the only places that mutate s.data, and both
// keep keysWithTTL in sync — every other method must go through them
// rather than touching s.data directly.
type Store struct {
	mu          sync.RWMutex
	data        map[string]entry
	keysWithTTL map[string]struct{}
}

// New returns an empty, ready-to-use Store. Active expiry is not started
// automatically; call StartActiveExpiry if it's wanted.
func New() *Store {
	return &Store{
		data:        make(map[string]entry),
		keysWithTTL: make(map[string]struct{}),
	}
}

// expired reports whether e's TTL (if any) has passed as of now.
func expired(e entry, now time.Time) bool {
	return !e.expireAt.IsZero() && !e.expireAt.After(now)
}

// lookup returns the live (non-expired) entry at key, treating an expired
// key as absent. Callers holding only the read lock must pass
// purge=false, since reclaiming the expired entry from the maps requires
// the write lock; callers holding the write lock should pass purge=true
// so expired keys are cleaned up as they're discovered (this is the lazy
// half of expiry).
func (s *Store) lookup(key string, purge bool) (entry, bool) {
	e, ok := s.data[key]
	if !ok {
		return entry{}, false
	}
	if expired(e, time.Now()) {
		if purge {
			s.deleteKey(key)
		}
		return entry{}, false
	}
	return e, true
}

// setEntry stores e under key and keeps keysWithTTL in sync with e's TTL.
// Every write of s.data must go through this (or deleteKey) rather than
// assigning s.data[key] directly, or the active-expiry sample set would
// drift out of sync with reality. Callers must hold the write lock.
func (s *Store) setEntry(key string, e entry) {
	s.data[key] = e
	if e.expireAt.IsZero() {
		delete(s.keysWithTTL, key)
	} else {
		s.keysWithTTL[key] = struct{}{}
	}
}

// deleteKey removes key from the store entirely. Callers must hold the
// write lock.
func (s *Store) deleteKey(key string) {
	delete(s.data, key)
	delete(s.keysWithTTL, key)
}

// --- Expiry ---

// Expire sets key's remaining time to live to ttl, matching Redis's
// EXPIRE. If ttl is zero or negative, key is deleted immediately (Redis
// treats an already-past expiry as an immediate delete). It reports
// whether key existed. Expire applies regardless of key's type.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
	if !ok {
		return false
	}
	if ttl <= 0 {
		s.deleteKey(key)
		return true
	}
	e.expireAt = time.Now().Add(ttl)
	s.setEntry(key, e)
	return true
}

// TTLSeconds returns key's remaining time to live in whole seconds
// (rounded up), following Redis's TTL command semantics: -2 if key
// doesn't exist (or has already expired), -1 if key exists but has no
// TTL, otherwise the number of seconds remaining.
func (s *Store) TTLSeconds(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
	if !ok {
		return -2
	}
	if e.expireAt.IsZero() {
		return -1
	}
	remaining := time.Until(e.expireAt)
	if remaining < 0 {
		remaining = 0
	}
	return int64(math.Ceil(remaining.Seconds()))
}

// Persist removes key's TTL, if it has one, making it persist forever
// until otherwise deleted. It reports whether a TTL was actually removed
// (false if key doesn't exist or already had no TTL), matching Redis's
// PERSIST return value. Persist applies regardless of key's type.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
	if !ok || e.expireAt.IsZero() {
		return false
	}
	e.expireAt = time.Time{}
	s.setEntry(key, e)
	return true
}

// StartActiveExpiry launches a background goroutine that wakes every
// interval and sweeps a bounded sample of keys with a TTL set, deleting
// any that have expired — Redis's "active expire cycle", reclaiming
// memory for expired keys that are never accessed again (lazy expiry
// alone only hides them on access; it never frees them). It returns a
// stop function that halts the goroutine; callers should call it during
// shutdown, and tests that start active expiry must call it to avoid
// leaking the goroutine.
func (s *Store) StartActiveExpiry(interval time.Duration) (stop func()) {
	stopCh := make(chan struct{})
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				s.sweepExpired()
			}
		}
	}()
	return func() { close(stopCh) }
}

// activeExpirySampleSize bounds how many keys-with-TTL a single sweep
// examines, mirroring Redis's own sampling-based active expire cycle
// (which also examines a bounded sample per pass) rather than scanning
// the whole keyspace on every tick.
const activeExpirySampleSize = 20

// sweepExpired examines a sample of keysWithTTL and deletes any that have
// expired. Go's map iteration order is randomized per-iteration by the
// runtime, so simply taking the first activeExpirySampleSize entries
// visited already gives a pseudo-random sample without any extra
// bookkeeping.
func (s *Store) sweepExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	sampled := 0
	for key := range s.keysWithTTL {
		if sampled >= activeExpirySampleSize {
			break
		}
		sampled++
		if e, ok := s.data[key]; ok && expired(e, now) {
			s.deleteKey(key)
		}
	}
}

// --- Strings ---

// Set stores value as a string under key, overwriting any existing value
// regardless of its type and clearing any TTL it had — matching Redis,
// where a plain SET always replaces whatever was there and drops any
// prior expiry. Use SetEx for SET ... EX/PX.
func (s *Store) Set(key, value string) {
	s.SetEx(key, value, 0)
}

// SetEx is SET key value with an optional expiry (SET ... EX/PX). A zero
// or negative ttl means no expiry, matching plain SET.
func (s *Store) SetEx(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := entry{kind: kindString, str: value}
	if ttl > 0 {
		e.expireAt = time.Now().Add(ttl)
	}
	s.setEntry(key, e)
}

// Get returns the string stored at key and whether it was present. It
// fails with errWrongType if key holds a hash, list, or zset.
func (s *Store) Get(key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
	if !ok {
		return "", false, nil
	}
	if e.kind != kindString {
		return "", false, errWrongType
	}
	return e.str, true, nil
}

// Del removes each of keys, if present, regardless of type, and returns
// how many were actually deleted (Redis's DEL return value). An expired
// key is treated as already absent and doesn't count.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, k := range keys {
		if _, ok := s.lookup(k, true); ok {
			s.deleteKey(k)
			count++
		}
	}
	return count
}

// Exists returns how many of keys are currently present, regardless of
// type. Redis counts a key multiple times if it's repeated in the
// argument list, so this does too. An expired key doesn't count.
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, k := range keys {
		if _, ok := s.lookup(k, false); ok {
			count++
		}
	}
	return count
}

// IncrBy atomically adds delta to the integer value stored at key (treating
// a missing key as 0) and stores + returns the result. It fails if key
// holds a hash, list, or zset, if the existing string value isn't a
// base-10 int64, or if the addition would overflow int64 — all matching
// Redis's INCR/DECR/INCRBY/DECRBY behavior. Any existing TTL on key is
// preserved (INCR modifies a value in place; it isn't SET).
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var cur int64
	var expireAt time.Time
	if e, ok := s.lookup(key, true); ok {
		if e.kind != kindString {
			return 0, errWrongType
		}
		n, err := strconv.ParseInt(e.str, 10, 64)
		if err != nil {
			return 0, errNotInteger
		}
		cur = n
		expireAt = e.expireAt
	}

	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, fmt.Errorf("ERR increment or decrement would overflow")
	}

	next := cur + delta
	s.setEntry(key, entry{kind: kindString, str: strconv.FormatInt(next, 10), expireAt: expireAt})
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
// hash, list, or zset. Any existing TTL on key is preserved.
func (s *Store) Append(key, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
	if ok && e.kind != kindString {
		return 0, errWrongType
	}
	next := e.str + value
	s.setEntry(key, entry{kind: kindString, str: next, expireAt: e.expireAt})
	return len(next), nil
}

// Strlen returns the length of the string stored at key, or 0 if key
// doesn't exist, matching Redis's STRLEN. It fails with errWrongType if
// key holds a hash, list, or zset.
func (s *Store) Strlen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
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
// errWrongType if key holds a string, list, or zset.
func (s *Store) HSet(key string, pairs ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.lookup(key, true)
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
	s.setEntry(key, e)
	return added, nil
}

// HGet returns the value of field in the hash at key, and whether it was
// present. A missing key behaves like an empty hash (ok=false, no error).
// It fails with errWrongType if key holds a string, list, or zset.
func (s *Store) HGet(key, field string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
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
// with errWrongType if key holds a string, list, or zset.
func (s *Store) HDel(key string, fields ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
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
		s.deleteKey(key)
	} else {
		s.setEntry(key, e)
	}
	return count, nil
}

// HGetAll returns a copy of all field/value pairs in the hash at key, or
// an empty map if key doesn't exist. It fails with errWrongType if key
// holds a string, list, or zset.
func (s *Store) HGetAll(key string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
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
// with errWrongType if key holds a string, list, or zset.
func (s *Store) HExists(key, field string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
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
// doesn't exist. It fails with errWrongType if key holds a string, list,
// or zset.
func (s *Store) HLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
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
// string, hash, or zset.
func (s *Store) LPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
	if ok && e.kind != kindList {
		return 0, errWrongType
	}
	if !ok {
		e = entry{kind: kindList}
	}
	for _, v := range values {
		e.list = append([]string{v}, e.list...)
	}
	s.setEntry(key, e)
	return len(e.list), nil
}

// RPush pushes each of values onto the tail (right) of the list at key, in
// order, and returns the list's new length, matching Redis's RPUSH. It
// creates the list if key doesn't exist, and fails with errWrongType if
// key holds a string, hash, or zset.
func (s *Store) RPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
	if ok && e.kind != kindList {
		return 0, errWrongType
	}
	if !ok {
		e = entry{kind: kindList}
	}
	e.list = append(e.list, values...)
	s.setEntry(key, e)
	return len(e.list), nil
}

// LPop removes and returns the first element of the list at key. ok is
// false if key doesn't exist or the list is empty (Redis returns nil in
// both cases). If the list becomes empty, the key itself is removed. It
// fails with errWrongType if key holds a string, hash, or zset.
func (s *Store) LPop(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
	if !ok {
		return "", false, nil
	}
	if e.kind != kindList {
		return "", false, errWrongType
	}
	if len(e.list) == 0 {
		s.deleteKey(key)
		return "", false, nil
	}
	v := e.list[0]
	e.list = e.list[1:]
	if len(e.list) == 0 {
		s.deleteKey(key)
	} else {
		s.setEntry(key, e)
	}
	return v, true, nil
}

// RPop removes and returns the last element of the list at key. ok is
// false if key doesn't exist or the list is empty (Redis returns nil in
// both cases). If the list becomes empty, the key itself is removed. It
// fails with errWrongType if key holds a string, hash, or zset.
func (s *Store) RPop(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
	if !ok {
		return "", false, nil
	}
	if e.kind != kindList {
		return "", false, errWrongType
	}
	if len(e.list) == 0 {
		s.deleteKey(key)
		return "", false, nil
	}
	last := len(e.list) - 1
	v := e.list[last]
	e.list = e.list[:last]
	if len(e.list) == 0 {
		s.deleteKey(key)
	} else {
		s.setEntry(key, e)
	}
	return v, true, nil
}

// LRange returns the elements of the list at key between start and stop,
// inclusive, matching Redis's LRANGE index semantics: negative indices
// count from the end of the list (-1 is the last element), and an
// out-of-range or empty result yields an empty (non-nil) slice rather than
// an error. A missing key behaves like an empty list. It fails with
// errWrongType if key holds a string, hash, or zset.
func (s *Store) LRange(key string, start, stop int64) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
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
// It fails with errWrongType if key holds a string, hash, or zset.
func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
	if !ok {
		return 0, nil
	}
	if e.kind != kindList {
		return 0, errWrongType
	}
	return len(e.list), nil
}

// --- Sorted sets ---

// ZAddPair is one (score, member) pair passed to ZAdd.
type ZAddPair struct {
	Score  float64
	Member string
}

// ZAdd sets each pair's member to its score in the sorted set at key,
// creating the set if needed, and returns how many members were newly
// added (not merely re-scored) — matching Redis's ZADD return value. If a
// member already exists, its skip list entry is removed and reinserted at
// the new score (a no-op if the score is unchanged), keeping the skip
// list's ordering and the member->score map in sync. It fails with
// errWrongType if key holds a non-zset type.
func (s *Store) ZAdd(key string, pairs ...ZAddPair) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.lookup(key, true)
	if ok && e.kind != kindZSet {
		return 0, errWrongType
	}
	if !ok {
		e = entry{kind: kindZSet, zset: newZSet()}
	}

	added := 0
	for _, pr := range pairs {
		if oldScore, exists := e.zset.scores[pr.Member]; exists {
			if oldScore == pr.Score {
				continue
			}
			e.zset.sl.Delete(oldScore, pr.Member)
			e.zset.sl.Insert(pr.Score, pr.Member)
			e.zset.scores[pr.Member] = pr.Score
		} else {
			e.zset.sl.Insert(pr.Score, pr.Member)
			e.zset.scores[pr.Member] = pr.Score
			added++
		}
	}
	s.setEntry(key, e)
	return added, nil
}

// ZScore returns member's score in the sorted set at key, and whether it
// was present. It fails with errWrongType if key holds a non-zset type.
func (s *Store) ZScore(key, member string) (float64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
	if !ok {
		return 0, false, nil
	}
	if e.kind != kindZSet {
		return 0, false, errWrongType
	}
	score, ok := e.zset.scores[member]
	return score, ok, nil
}

// ZRank returns member's 0-based rank in the sorted set at key, ordered by
// score ascending (ties broken by member name), and whether it was
// present. It fails with errWrongType if key holds a non-zset type.
func (s *Store) ZRank(key, member string) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
	if !ok {
		return 0, false, nil
	}
	if e.kind != kindZSet {
		return 0, false, errWrongType
	}
	score, ok := e.zset.scores[member]
	if !ok {
		return 0, false, nil
	}
	rank, ok := e.zset.sl.Rank(score, member)
	return rank, ok, nil
}

// ZRange returns the members of the sorted set at key between ranks start
// and stop, inclusive, ordered by score ascending (ties broken by member
// name), supporting negative indices (-1 is the last element) as Redis
// does. A missing key behaves like an empty set. It fails with
// errWrongType if key holds a non-zset type.
func (s *Store) ZRange(key string, start, stop int64) ([]ZMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, false)
	if !ok {
		return []ZMember{}, nil
	}
	if e.kind != kindZSet {
		return nil, errWrongType
	}
	elems := e.zset.sl.Range(int(start), int(stop))
	result := make([]ZMember, len(elems))
	for i, el := range elems {
		result[i] = ZMember{Member: el.Member, Score: el.Score}
	}
	return result, nil
}

// ZRem removes each of members from the sorted set at key and returns how
// many were actually removed. If the set becomes empty, the key itself is
// removed, matching Redis. It fails with errWrongType if key holds a
// non-zset type.
func (s *Store) ZRem(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, true)
	if !ok {
		return 0, nil
	}
	if e.kind != kindZSet {
		return 0, errWrongType
	}

	count := 0
	for _, m := range members {
		score, exists := e.zset.scores[m]
		if !exists {
			continue
		}
		e.zset.sl.Delete(score, m)
		delete(e.zset.scores, m)
		count++
	}
	if len(e.zset.scores) == 0 {
		s.deleteKey(key)
	} else {
		s.setEntry(key, e)
	}
	return count, nil
}

// ZIncrBy adds delta to member's score in the sorted set at key (creating
// the set, and the member with a starting score of 0, if either doesn't
// exist yet) and returns the new score, matching Redis's ZINCRBY. It fails
// with errWrongType if key holds a non-zset type.
func (s *Store) ZIncrBy(key string, delta float64, member string) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.lookup(key, true)
	if ok && e.kind != kindZSet {
		return 0, errWrongType
	}
	if !ok {
		e = entry{kind: kindZSet, zset: newZSet()}
	}

	oldScore, exists := e.zset.scores[member]
	newScore := oldScore + delta
	if exists {
		e.zset.sl.Delete(oldScore, member)
	}
	e.zset.sl.Insert(newScore, member)
	e.zset.scores[member] = newScore
	s.setEntry(key, e)
	return newScore, nil
}
