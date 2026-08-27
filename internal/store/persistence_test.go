package store

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

// populate fills s with one key of every type, including a string with a
// live TTL, so round-trip tests exercise every code path.
func populate(t *testing.T, s *Store) {
	t.Helper()
	s.SetEx("str", "hello", time.Hour)
	s.HSet("hash", "f1", "v1", "f2", "v2")
	s.RPush("list", "a", "b", "c")
	s.ZAdd("zset", ZAddPair{Score: 1, Member: "a"}, ZAddPair{Score: 2, Member: "b"})
}

// assertPopulated checks that s holds exactly what populate put there.
func assertPopulated(t *testing.T, s *Store) {
	t.Helper()

	str, ok, err := s.Get("str")
	if err != nil || !ok || str != "hello" {
		t.Errorf("Get(str) = %q, %v, %v, want hello, true, nil", str, ok, err)
	}
	ttl := s.TTLSeconds("str")
	if ttl <= 0 || ttl > 3600 {
		t.Errorf("TTLSeconds(str) = %d, want in (0, 3600] (TTL survived round trip)", ttl)
	}

	hash, err := s.HGetAll("hash")
	wantHash := map[string]string{"f1": "v1", "f2": "v2"}
	if err != nil || !reflect.DeepEqual(hash, wantHash) {
		t.Errorf("HGetAll(hash) = %v, %v, want %v, nil", hash, err, wantHash)
	}

	list, err := s.LRange("list", 0, -1)
	wantList := []string{"a", "b", "c"}
	if err != nil || !reflect.DeepEqual(list, wantList) {
		t.Errorf("LRange(list) = %v, %v, want %v, nil", list, err, wantList)
	}

	zrange, err := s.ZRange("zset", 0, -1)
	wantZ := []ZMember{{"a", 1}, {"b", 2}}
	if err != nil || !reflect.DeepEqual(zrange, wantZ) {
		t.Errorf("ZRange(zset) = %v, %v, want %v, nil", zrange, err, wantZ)
	}
	// Rank must also work post-restore, proving the skiplist (not just
	// the scores map) was correctly rebuilt.
	rank, ok, err := s.ZRank("zset", "b")
	if err != nil || !ok || rank != 1 {
		t.Errorf("ZRank(zset, b) = %d, %v, %v, want 1, true, nil", rank, ok, err)
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := New()
	populate(t, s)

	snap := s.Snapshot()

	restored := New()
	restored.Restore(snap)
	assertPopulated(t, restored)
}

func TestSnapshotSkipsExpiredKeys(t *testing.T) {
	s := New()
	s.SetEx("expired", "v", 1*time.Millisecond)
	s.Set("alive", "v")
	time.Sleep(5 * time.Millisecond)

	snap := s.Snapshot()

	var keys []string
	for _, k := range snap.Keys {
		keys = append(keys, k.Key)
	}
	sort.Strings(keys)
	if want := []string{"alive"}; !reflect.DeepEqual(keys, want) {
		t.Errorf("Snapshot().Keys = %v, want %v (expired key excluded)", keys, want)
	}
}

func TestSaveLoadFileRoundTrip(t *testing.T) {
	s := New()
	populate(t, s)

	path := filepath.Join(t.TempDir(), "dump.rdb")
	if err := s.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile: %v", err)
	}

	loaded := New()
	if err := loaded.LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	assertPopulated(t, loaded)
}

func TestSaveToFileIsAtomic(t *testing.T) {
	// After a successful save, no stray .tmp-* files should be left
	// behind in the target directory.
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.rdb")

	s := New()
	s.Set("k", "v")
	if err := s.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "dump.rdb" {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("directory contents after SaveToFile = %v, want exactly [dump.rdb]", names)
	}
}

func TestLoadFromFileMissing(t *testing.T) {
	s := New()
	err := s.LoadFromFile(filepath.Join(t.TempDir(), "nonexistent.rdb"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("LoadFromFile(missing) error = %v, want one satisfying errors.Is(_, os.ErrNotExist)", err)
	}
}

func TestLoadFromFileReplacesExistingContents(t *testing.T) {
	s := New()
	s.Set("old-key", "old-value")

	other := New()
	other.Set("new-key", "new-value")
	path := filepath.Join(t.TempDir(), "dump.rdb")
	if err := other.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile: %v", err)
	}

	if err := s.LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if _, ok, _ := s.Get("old-key"); ok {
		t.Error("old-key still present after LoadFromFile, want it replaced entirely")
	}
	if v, ok, _ := s.Get("new-key"); !ok || v != "new-value" {
		t.Errorf("Get(new-key) = %q, %v, want new-value, true", v, ok)
	}
}
