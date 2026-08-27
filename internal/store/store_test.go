package store

import (
	"math"
	"reflect"
	"testing"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("k", "v")
	got, ok, err := s.Get("k")
	if err != nil || !ok || got != "v" {
		t.Errorf("Get(k) = %q, %v, %v, want %q, true, nil", got, ok, err, "v")
	}

	s.Set("k", "v2")
	got, ok, err = s.Get("k")
	if err != nil || !ok || got != "v2" {
		t.Errorf("Get(k) after overwrite = %q, %v, %v, want %q, true, nil", got, ok, err, "v2")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	if _, ok, err := s.Get("nope"); ok || err != nil {
		t.Errorf("Get(missing) = _, %v, %v, want false, nil", ok, err)
	}
}

func TestDel(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")

	if n := s.Del("a", "b", "c"); n != 2 {
		t.Errorf("Del(a, b, c) = %d, want 2", n)
	}
	if _, ok, _ := s.Get("a"); ok {
		t.Error("a still present after Del")
	}
	if n := s.Del("a"); n != 0 {
		t.Errorf("Del(already-deleted) = %d, want 0", n)
	}
}

func TestExists(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")

	if n := s.Exists("a", "b", "c"); n != 2 {
		t.Errorf("Exists(a, b, c) = %d, want 2", n)
	}
	// Redis counts a repeated key once per occurrence in the arg list.
	if n := s.Exists("a", "a"); n != 2 {
		t.Errorf("Exists(a, a) = %d, want 2", n)
	}
}

func TestIncrDecr(t *testing.T) {
	s := New()

	n, err := s.Incr("counter")
	if err != nil || n != 1 {
		t.Fatalf("Incr(missing key) = %d, %v, want 1, nil", n, err)
	}

	n, err = s.Incr("counter")
	if err != nil || n != 2 {
		t.Fatalf("Incr(counter) = %d, %v, want 2, nil", n, err)
	}

	n, err = s.Decr("counter")
	if err != nil || n != 1 {
		t.Fatalf("Decr(counter) = %d, %v, want 1, nil", n, err)
	}

	n, err = s.IncrBy("counter", 10)
	if err != nil || n != 11 {
		t.Fatalf("IncrBy(counter, 10) = %d, %v, want 11, nil", n, err)
	}
}

func TestIncrNonInteger(t *testing.T) {
	s := New()
	s.Set("k", "not a number")

	if _, err := s.Incr("k"); err == nil {
		t.Error("Incr(non-integer value) err = nil, want error")
	}
	if _, err := s.Decr("k"); err == nil {
		t.Error("Decr(non-integer value) err = nil, want error")
	}
}

func TestIncrOverflow(t *testing.T) {
	s := New()
	s.Set("k", "9223372036854775807") // math.MaxInt64

	if _, err := s.Incr("k"); err == nil {
		t.Error("Incr(MaxInt64) err = nil, want overflow error")
	}

	s.Set("k", "-9223372036854775808") // math.MinInt64
	if _, err := s.Decr("k"); err == nil {
		t.Error("Decr(MinInt64) err = nil, want overflow error")
	}

	s.Set("k", "0")
	if n, err := s.IncrBy("k", math.MaxInt64); err != nil || n != math.MaxInt64 {
		t.Errorf("IncrBy(0, MaxInt64) = %d, %v, want %d, nil", n, err, int64(math.MaxInt64))
	}
}

func TestAppend(t *testing.T) {
	s := New()

	n, err := s.Append("k", "hello")
	if err != nil || n != 5 {
		t.Errorf("Append(missing key, hello) = %d, %v, want 5, nil", n, err)
	}

	n, err = s.Append("k", " world")
	if err != nil || n != 11 {
		t.Errorf("Append(k, ' world') = %d, %v, want 11, nil", n, err)
	}

	got, _, _ := s.Get("k")
	if got != "hello world" {
		t.Errorf("Get(k) after appends = %q, want %q", got, "hello world")
	}
}

func TestStrlen(t *testing.T) {
	s := New()
	if n, err := s.Strlen("missing"); err != nil || n != 0 {
		t.Errorf("Strlen(missing) = %d, %v, want 0, nil", n, err)
	}

	s.Set("k", "hello")
	if n, err := s.Strlen("k"); err != nil || n != 5 {
		t.Errorf("Strlen(k) = %d, %v, want 5, nil", n, err)
	}
}

func TestConcurrentIncr(t *testing.T) {
	s := New()
	const goroutines = 50
	const perGoroutine = 100

	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < perGoroutine; j++ {
				if _, err := s.Incr("counter"); err != nil {
					t.Error(err)
				}
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}

	got, _, _ := s.Get("counter")
	want := "5000"
	if got != want {
		t.Errorf("counter after concurrent increments = %q, want %q", got, want)
	}
}

// --- Hashes ---

func TestHSetHGet(t *testing.T) {
	s := New()

	n, err := s.HSet("h", "f1", "v1")
	if err != nil || n != 1 {
		t.Fatalf("HSet(new field) = %d, %v, want 1, nil", n, err)
	}

	v, ok, err := s.HGet("h", "f1")
	if err != nil || !ok || v != "v1" {
		t.Errorf("HGet(h, f1) = %q, %v, %v, want v1, true, nil", v, ok, err)
	}

	// Overwriting an existing field returns 0 new fields.
	n, err = s.HSet("h", "f1", "v2")
	if err != nil || n != 0 {
		t.Fatalf("HSet(overwrite) = %d, %v, want 0, nil", n, err)
	}
	v, _, _ = s.HGet("h", "f1")
	if v != "v2" {
		t.Errorf("HGet(h, f1) after overwrite = %q, want v2", v)
	}

	// Multiple pairs in one call.
	n, err = s.HSet("h", "f2", "v2", "f3", "v3")
	if err != nil || n != 2 {
		t.Fatalf("HSet(two new fields) = %d, %v, want 2, nil", n, err)
	}
}

func TestHGetMissing(t *testing.T) {
	s := New()
	if _, ok, err := s.HGet("missing", "f"); ok || err != nil {
		t.Errorf("HGet(missing key) = _, %v, %v, want false, nil", ok, err)
	}

	s.HSet("h", "f1", "v1")
	if _, ok, err := s.HGet("h", "missing-field"); ok || err != nil {
		t.Errorf("HGet(missing field) = _, %v, %v, want false, nil", ok, err)
	}
}

func TestHDel(t *testing.T) {
	s := New()
	s.HSet("h", "f1", "v1", "f2", "v2")

	n, err := s.HDel("h", "f1", "nope")
	if err != nil || n != 1 {
		t.Fatalf("HDel(h, f1, nope) = %d, %v, want 1, nil", n, err)
	}
	if _, ok, _ := s.HGet("h", "f1"); ok {
		t.Error("f1 still present after HDel")
	}

	// Deleting the last field removes the key entirely.
	if _, err := s.HDel("h", "f2"); err != nil {
		t.Fatalf("HDel(h, f2): %v", err)
	}
	if n, _ := s.HLen("h"); n != 0 {
		t.Errorf("HLen(h) after deleting all fields = %d, want 0", n)
	}
	if s.Exists("h") != 0 {
		t.Error("h key still exists after all fields deleted")
	}
}

func TestHGetAll(t *testing.T) {
	s := New()

	got, err := s.HGetAll("missing")
	if err != nil || len(got) != 0 {
		t.Errorf("HGetAll(missing) = %v, %v, want empty map, nil", got, err)
	}

	s.HSet("h", "f1", "v1", "f2", "v2")
	got, err = s.HGetAll("h")
	want := map[string]string{"f1": "v1", "f2": "v2"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("HGetAll(h) = %v, %v, want %v, nil", got, err, want)
	}

	// Returned map must be a copy, not a live view.
	got["f1"] = "mutated"
	v, _, _ := s.HGet("h", "f1")
	if v != "v1" {
		t.Error("HGetAll result aliases internal storage")
	}
}

func TestHExists(t *testing.T) {
	s := New()
	s.HSet("h", "f1", "v1")

	if ok, err := s.HExists("h", "f1"); err != nil || !ok {
		t.Errorf("HExists(h, f1) = %v, %v, want true, nil", ok, err)
	}
	if ok, err := s.HExists("h", "nope"); err != nil || ok {
		t.Errorf("HExists(h, nope) = %v, %v, want false, nil", ok, err)
	}
	if ok, err := s.HExists("missing", "f"); err != nil || ok {
		t.Errorf("HExists(missing, f) = %v, %v, want false, nil", ok, err)
	}
}

func TestHLen(t *testing.T) {
	s := New()
	if n, err := s.HLen("missing"); err != nil || n != 0 {
		t.Errorf("HLen(missing) = %d, %v, want 0, nil", n, err)
	}

	s.HSet("h", "f1", "v1", "f2", "v2")
	if n, err := s.HLen("h"); err != nil || n != 2 {
		t.Errorf("HLen(h) = %d, %v, want 2, nil", n, err)
	}
}

// --- Lists ---

func TestLPushRPush(t *testing.T) {
	s := New()

	n, err := s.RPush("l", "a", "b", "c")
	if err != nil || n != 3 {
		t.Fatalf("RPush(l, a, b, c) = %d, %v, want 3, nil", n, err)
	}
	got, err := s.LRange("l", 0, -1)
	if err != nil || !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("LRange after RPush = %v, %v, want [a b c], nil", got, err)
	}

	n, err = s.LPush("l2", "a", "b", "c")
	if err != nil || n != 3 {
		t.Fatalf("LPush(l2, a, b, c) = %d, %v, want 3, nil", n, err)
	}
	// LPush pushes one at a time, so the last argument ends up at the head.
	got, err = s.LRange("l2", 0, -1)
	if err != nil || !reflect.DeepEqual(got, []string{"c", "b", "a"}) {
		t.Errorf("LRange after LPush = %v, %v, want [c b a], nil", got, err)
	}
}

func TestLPopRPop(t *testing.T) {
	s := New()
	s.RPush("l", "a", "b", "c")

	v, ok, err := s.LPop("l")
	if err != nil || !ok || v != "a" {
		t.Fatalf("LPop(l) = %q, %v, %v, want a, true, nil", v, ok, err)
	}

	v, ok, err = s.RPop("l")
	if err != nil || !ok || v != "c" {
		t.Fatalf("RPop(l) = %q, %v, %v, want c, true, nil", v, ok, err)
	}

	// One element left ("b"); popping it should remove the key entirely.
	v, ok, err = s.LPop("l")
	if err != nil || !ok || v != "b" {
		t.Fatalf("LPop(l) last element = %q, %v, %v, want b, true, nil", v, ok, err)
	}
	if s.Exists("l") != 0 {
		t.Error("l key still exists after popping all elements")
	}
}

func TestLPopRPopEmptyOrMissing(t *testing.T) {
	s := New()

	if v, ok, err := s.LPop("missing"); ok || err != nil || v != "" {
		t.Errorf("LPop(missing) = %q, %v, %v, want \"\", false, nil", v, ok, err)
	}
	if v, ok, err := s.RPop("missing"); ok || err != nil || v != "" {
		t.Errorf("RPop(missing) = %q, %v, %v, want \"\", false, nil", v, ok, err)
	}
}

func TestLRangeNegativeIndices(t *testing.T) {
	s := New()
	s.RPush("l", "a", "b", "c", "d", "e")

	tests := []struct {
		name        string
		start, stop int64
		want        []string
	}{
		{"whole list", 0, -1, []string{"a", "b", "c", "d", "e"}},
		{"last element", -1, -1, []string{"e"}},
		{"last three", -3, -1, []string{"c", "d", "e"}},
		{"middle slice", 1, 3, []string{"b", "c", "d"}},
		{"stop past end clamps", 0, 100, []string{"a", "b", "c", "d", "e"}},
		{"start past end is empty", 10, -1, []string{}},
		{"start after stop is empty", 3, 1, []string{}},
		{"very negative start clamps to 0", -100, 1, []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.LRange("l", tt.start, tt.stop)
			if err != nil {
				t.Fatalf("LRange(%d, %d): %v", tt.start, tt.stop, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LRange(%d, %d) = %v, want %v", tt.start, tt.stop, got, tt.want)
			}
		})
	}
}

func TestLRangeMissingKey(t *testing.T) {
	s := New()
	got, err := s.LRange("missing", 0, -1)
	if err != nil || !reflect.DeepEqual(got, []string{}) {
		t.Errorf("LRange(missing) = %v, %v, want [], nil", got, err)
	}
}

func TestLLen(t *testing.T) {
	s := New()
	if n, err := s.LLen("missing"); err != nil || n != 0 {
		t.Errorf("LLen(missing) = %d, %v, want 0, nil", n, err)
	}

	s.RPush("l", "a", "b", "c")
	if n, err := s.LLen("l"); err != nil || n != 3 {
		t.Errorf("LLen(l) = %d, %v, want 3, nil", n, err)
	}
}

// --- WRONGTYPE ---

func TestWrongTypeOnStrings(t *testing.T) {
	s := New()
	s.HSet("hash", "f", "v")
	s.RPush("list", "a")

	for _, key := range []string{"hash", "list"} {
		if _, _, err := s.Get(key); err == nil {
			t.Errorf("Get(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.IncrBy(key, 1); err == nil {
			t.Errorf("IncrBy(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.Append(key, "x"); err == nil {
			t.Errorf("Append(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.Strlen(key); err == nil {
			t.Errorf("Strlen(%s) err = nil, want WRONGTYPE", key)
		}
	}
}

func TestWrongTypeOnHashes(t *testing.T) {
	s := New()
	s.Set("str", "v")
	s.RPush("list", "a")

	for _, key := range []string{"str", "list"} {
		if _, err := s.HSet(key, "f", "v"); err == nil {
			t.Errorf("HSet(%s) err = nil, want WRONGTYPE", key)
		}
		if _, _, err := s.HGet(key, "f"); err == nil {
			t.Errorf("HGet(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.HDel(key, "f"); err == nil {
			t.Errorf("HDel(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.HGetAll(key); err == nil {
			t.Errorf("HGetAll(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.HExists(key, "f"); err == nil {
			t.Errorf("HExists(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.HLen(key); err == nil {
			t.Errorf("HLen(%s) err = nil, want WRONGTYPE", key)
		}
	}
}

func TestWrongTypeOnLists(t *testing.T) {
	s := New()
	s.Set("str", "v")
	s.HSet("hash", "f", "v")

	for _, key := range []string{"str", "hash"} {
		if _, err := s.LPush(key, "x"); err == nil {
			t.Errorf("LPush(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.RPush(key, "x"); err == nil {
			t.Errorf("RPush(%s) err = nil, want WRONGTYPE", key)
		}
		if _, _, err := s.LPop(key); err == nil {
			t.Errorf("LPop(%s) err = nil, want WRONGTYPE", key)
		}
		if _, _, err := s.RPop(key); err == nil {
			t.Errorf("RPop(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.LRange(key, 0, -1); err == nil {
			t.Errorf("LRange(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.LLen(key); err == nil {
			t.Errorf("LLen(%s) err = nil, want WRONGTYPE", key)
		}
	}
}

// --- Sorted sets ---

func TestZAddZScore(t *testing.T) {
	s := New()

	n, err := s.ZAdd("z", ZAddPair{Score: 1, Member: "a"})
	if err != nil || n != 1 {
		t.Fatalf("ZAdd(new member) = %d, %v, want 1, nil", n, err)
	}

	score, ok, err := s.ZScore("z", "a")
	if err != nil || !ok || score != 1 {
		t.Errorf("ZScore(z, a) = %v, %v, %v, want 1, true, nil", score, ok, err)
	}

	// Re-adding the same member with a different score updates it and
	// reports 0 newly added.
	n, err = s.ZAdd("z", ZAddPair{Score: 5, Member: "a"})
	if err != nil || n != 0 {
		t.Fatalf("ZAdd(update score) = %d, %v, want 0, nil", n, err)
	}
	score, _, _ = s.ZScore("z", "a")
	if score != 5 {
		t.Errorf("ZScore(z, a) after update = %v, want 5", score)
	}

	// Multiple pairs in one call.
	n, err = s.ZAdd("z", ZAddPair{Score: 2, Member: "b"}, ZAddPair{Score: 3, Member: "c"})
	if err != nil || n != 2 {
		t.Fatalf("ZAdd(two new members) = %d, %v, want 2, nil", n, err)
	}
}

func TestZScoreMissing(t *testing.T) {
	s := New()
	if _, ok, err := s.ZScore("missing", "m"); ok || err != nil {
		t.Errorf("ZScore(missing key) = _, %v, %v, want false, nil", ok, err)
	}
	s.ZAdd("z", ZAddPair{Score: 1, Member: "a"})
	if _, ok, err := s.ZScore("z", "nope"); ok || err != nil {
		t.Errorf("ZScore(missing member) = _, %v, %v, want false, nil", ok, err)
	}
}

func TestZRank(t *testing.T) {
	s := New()
	s.ZAdd("z",
		ZAddPair{Score: 30, Member: "c"},
		ZAddPair{Score: 10, Member: "a"},
		ZAddPair{Score: 20, Member: "b"},
	)

	tests := []struct {
		member   string
		wantRank int
	}{
		{"a", 0},
		{"b", 1},
		{"c", 2},
	}
	for _, tt := range tests {
		rank, ok, err := s.ZRank("z", tt.member)
		if err != nil || !ok || rank != tt.wantRank {
			t.Errorf("ZRank(z, %s) = %d, %v, %v, want %d, true, nil", tt.member, rank, ok, err, tt.wantRank)
		}
	}

	if _, ok, err := s.ZRank("z", "nope"); ok || err != nil {
		t.Errorf("ZRank(missing member) = _, %v, %v, want false, nil", ok, err)
	}
	if _, ok, err := s.ZRank("missing", "a"); ok || err != nil {
		t.Errorf("ZRank(missing key) = _, %v, %v, want false, nil", ok, err)
	}
}

func TestZRange(t *testing.T) {
	s := New()
	s.ZAdd("z",
		ZAddPair{Score: 1, Member: "a"},
		ZAddPair{Score: 2, Member: "b"},
		ZAddPair{Score: 3, Member: "c"},
	)

	got, err := s.ZRange("z", 0, -1)
	want := []ZMember{{"a", 1}, {"b", 2}, {"c", 3}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("ZRange(z, 0, -1) = %v, %v, want %v, nil", got, err, want)
	}

	got, err = s.ZRange("z", -1, -1)
	want = []ZMember{{"c", 3}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("ZRange(z, -1, -1) = %v, %v, want %v, nil", got, err, want)
	}
}

func TestZRangeMissingKey(t *testing.T) {
	s := New()
	got, err := s.ZRange("missing", 0, -1)
	if err != nil || !reflect.DeepEqual(got, []ZMember{}) {
		t.Errorf("ZRange(missing) = %v, %v, want [], nil", got, err)
	}
}

func TestZRem(t *testing.T) {
	s := New()
	s.ZAdd("z", ZAddPair{Score: 1, Member: "a"}, ZAddPair{Score: 2, Member: "b"})

	n, err := s.ZRem("z", "a", "nope")
	if err != nil || n != 1 {
		t.Fatalf("ZRem(z, a, nope) = %d, %v, want 1, nil", n, err)
	}
	if _, ok, _ := s.ZScore("z", "a"); ok {
		t.Error("a still present after ZRem")
	}

	// Removing the last member removes the key entirely.
	if _, err := s.ZRem("z", "b"); err != nil {
		t.Fatalf("ZRem(z, b): %v", err)
	}
	if s.Exists("z") != 0 {
		t.Error("z key still exists after all members removed")
	}
}

func TestZIncrBy(t *testing.T) {
	s := New()

	score, err := s.ZIncrBy("z", 5, "a")
	if err != nil || score != 5 {
		t.Fatalf("ZIncrBy(missing member, 5) = %v, %v, want 5, nil", score, err)
	}

	score, err = s.ZIncrBy("z", 2.5, "a")
	if err != nil || score != 7.5 {
		t.Fatalf("ZIncrBy(a, 2.5) = %v, %v, want 7.5, nil", score, err)
	}

	score, err = s.ZIncrBy("z", -10, "a")
	if err != nil || score != -2.5 {
		t.Fatalf("ZIncrBy(a, -10) = %v, %v, want -2.5, nil", score, err)
	}

	// Rank/order must stay consistent after ZIncrBy changes a score.
	s.ZAdd("z", ZAddPair{Score: 0, Member: "b"})
	rank, ok, err := s.ZRank("z", "a")
	if err != nil || !ok || rank != 0 {
		t.Errorf("ZRank(z, a) after incr below b = %d, %v, %v, want 0, true, nil", rank, ok, err)
	}
}

func TestWrongTypeOnZSets(t *testing.T) {
	s := New()
	s.Set("str", "v")
	s.HSet("hash", "f", "v")
	s.RPush("list", "a")

	for _, key := range []string{"str", "hash", "list"} {
		if _, err := s.ZAdd(key, ZAddPair{Score: 1, Member: "m"}); err == nil {
			t.Errorf("ZAdd(%s) err = nil, want WRONGTYPE", key)
		}
		if _, _, err := s.ZScore(key, "m"); err == nil {
			t.Errorf("ZScore(%s) err = nil, want WRONGTYPE", key)
		}
		if _, _, err := s.ZRank(key, "m"); err == nil {
			t.Errorf("ZRank(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.ZRange(key, 0, -1); err == nil {
			t.Errorf("ZRange(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.ZRem(key, "m"); err == nil {
			t.Errorf("ZRem(%s) err = nil, want WRONGTYPE", key)
		}
		if _, err := s.ZIncrBy(key, 1, "m"); err == nil {
			t.Errorf("ZIncrBy(%s) err = nil, want WRONGTYPE", key)
		}
	}
}
