// RDB-style persistence: dumping the store's full contents to disk and
// reloading them, so data survives a process restart. This isn't
// byte-compatible with real Redis's RDB file format — it's a much
// simpler binary encoding (Go's encoding/gob) of the same idea: a
// point-in-time snapshot of every live key, written to a temp file and
// atomically renamed into place so a crash mid-write can never leave a
// corrupt snapshot behind.
package store

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DefaultRDBPath is the default snapshot file name, matching Redis's own
// default dbfilename ("dump.rdb"), resolved relative to the process's
// working directory.
const DefaultRDBPath = "dump.rdb"

// snapshotKey is the gob-serializable form of one key's entry. Unlike
// entry, every field is exported so gob's reflection-based encoding can
// see it. A sorted set is stored as a flat member/score list rather than
// its internal skiplist — the skiplist is trivially and cheaply rebuilt
// from that list on load, so there's no reason to serialize it directly.
type snapshotKey struct {
	Key      string
	Kind     int // kind(Kind) on restore; plain int avoids any doubt about gob and named types
	Str      string
	Hash     map[string]string
	List     []string
	ZSet     []ZMember
	ExpireAt time.Time
}

// snapshot is the gob-serializable form of an entire Store's contents.
type snapshot struct {
	Keys []snapshotKey
}

// Snapshot returns a point-in-time copy of the store's contents suitable
// for persisting to disk, silently skipping any key that has already
// expired (there's no reason to persist a key that lazy expiry would hide
// on the very next read anyway).
func (s *Store) Snapshot() snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	snap := snapshot{Keys: make([]snapshotKey, 0, len(s.data))}
	for key, e := range s.data {
		if expired(e, now) {
			continue
		}
		sk := snapshotKey{Key: key, Kind: int(e.kind), Str: e.str, ExpireAt: e.expireAt}
		switch e.kind {
		case kindHash:
			sk.Hash = make(map[string]string, len(e.hash))
			for f, v := range e.hash {
				sk.Hash[f] = v
			}
		case kindList:
			sk.List = append([]string(nil), e.list...)
		case kindZSet:
			sk.ZSet = make([]ZMember, 0, len(e.zset.scores))
			for member, score := range e.zset.scores {
				sk.ZSet = append(sk.ZSet, ZMember{Member: member, Score: score})
			}
		}
		snap.Keys = append(snap.Keys, sk)
	}
	return snap
}

// Restore replaces the store's entire contents with snap, rebuilding
// derived structures (each zset's skiplist) from the serialized
// member/score lists and re-populating keysWithTTL. Any key that had
// already expired by the time snap was taken (or has since expired) is
// dropped rather than restored.
func (s *Store) Restore(snap snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]entry, len(snap.Keys))
	s.keysWithTTL = make(map[string]struct{})

	now := time.Now()
	for _, sk := range snap.Keys {
		e := entry{kind: kind(sk.Kind), str: sk.Str, expireAt: sk.ExpireAt}
		if expired(e, now) {
			continue
		}
		switch e.kind {
		case kindHash:
			e.hash = sk.Hash
			if e.hash == nil {
				e.hash = make(map[string]string)
			}
		case kindList:
			e.list = sk.List
		case kindZSet:
			e.zset = newZSet()
			for _, m := range sk.ZSet {
				e.zset.sl.Insert(m.Score, m.Member)
				e.zset.scores[m.Member] = m.Score
			}
		}
		s.setEntry(sk.Key, e)
	}
}

// SaveToFile writes a snapshot of the store's current contents to path,
// matching Redis's SAVE. The snapshot is written to a temporary file in
// the same directory and then renamed into place, so a reader (or a
// crash) can never observe a half-written, truncated file at path — the
// rename is atomic on any filesystem Go supports this on.
func (s *Store) SaveToFile(path string) error {
	snap := s.Snapshot()

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("store: create temp snapshot file: %w", err)
	}
	tmpPath := tmp.Name()
	// If we return before the rename succeeds, clean up the temp file;
	// this is a no-op once the rename has already moved it to path.
	defer os.Remove(tmpPath)

	if err := gob.NewEncoder(tmp).Encode(snap); err != nil {
		tmp.Close()
		return fmt.Errorf("store: encode snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("store: close temp snapshot file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("store: rename snapshot into place: %w", err)
	}
	return nil
}

// LoadFromFile replaces the store's contents with the snapshot stored at
// path. If path doesn't exist, the returned error wraps os.ErrNotExist
// (checkable with errors.Is), so callers can treat "no snapshot yet" —
// e.g. the very first run — as a normal case rather than a failure.
func (s *Store) LoadFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err // preserves os.ErrNotExist for errors.Is
	}
	defer f.Close()

	var snap snapshot
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return fmt.Errorf("store: decode snapshot: %w", err)
	}
	s.Restore(snap)
	return nil
}
