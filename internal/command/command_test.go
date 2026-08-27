package command

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Ishan819/Redis-clone/internal/resp"
	"github.com/Ishan819/Redis-clone/internal/store"
)

func TestLookupIsCaseInsensitive(t *testing.T) {
	for _, name := range []string{"ping", "PING", "PiNg", "set", "SET", "get", "GET"} {
		if Lookup(name) == nil {
			t.Errorf("Lookup(%q) = nil, want a handler", name)
		}
	}
}

func TestLookupUnknownCommand(t *testing.T) {
	if h := Lookup("NOSUCHCOMMAND"); h != nil {
		t.Errorf("Lookup(unknown) = %v, want nil", h)
	}
}

func TestPing(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want resp.Value
	}{
		{"no args", nil, resp.SimpleStringValue("PONG")},
		{"one arg", []string{"hello"}, resp.BulkStringValue("hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ping(store.New(), tt.args)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ping(%v) = %+v, want %+v", tt.args, got, tt.want)
			}
		})
	}

	if got := ping(store.New(), []string{"too", "many"}); got.Type != resp.Error {
		t.Errorf("ping(too many args) = %+v, want an error reply", got)
	}
}

func TestEcho(t *testing.T) {
	want := resp.BulkStringValue("hello")
	if got := echo(store.New(), []string{"hello"}); !reflect.DeepEqual(got, want) {
		t.Errorf("echo([hello]) = %+v, want %+v", got, want)
	}

	if got := echo(store.New(), nil); got.Type != resp.Error {
		t.Errorf("echo(no args) = %+v, want an error reply", got)
	}
	if got := echo(store.New(), []string{"a", "b"}); got.Type != resp.Error {
		t.Errorf("echo(too many args) = %+v, want an error reply", got)
	}
}

func TestSetGet(t *testing.T) {
	s := store.New()

	if got := set(s, []string{"k", "v"}); !reflect.DeepEqual(got, resp.SimpleStringValue("OK")) {
		t.Errorf("set(k, v) = %+v, want OK", got)
	}

	want := resp.BulkStringValue("v")
	if got := get(s, []string{"k"}); !reflect.DeepEqual(got, want) {
		t.Errorf("get(k) = %+v, want %+v", got, want)
	}

	if got := set(s, []string{"only-one-arg"}); got.Type != resp.Error {
		t.Errorf("set(wrong arity) = %+v, want an error reply", got)
	}
	if got := get(s, nil); got.Type != resp.Error {
		t.Errorf("get(wrong arity) = %+v, want an error reply", got)
	}
}

func TestGetMissingKey(t *testing.T) {
	got := get(store.New(), []string{"missing"})
	want := resp.NullBulkString()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("get(missing) = %+v, want null bulk string %+v", got, want)
	}
}

func TestDel(t *testing.T) {
	s := store.New()
	set(s, []string{"a", "1"})
	set(s, []string{"b", "2"})

	got := del(s, []string{"a", "b", "c"})
	want := resp.IntegerValue(2)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("del(a, b, c) = %+v, want %+v", got, want)
	}

	if got := del(s, nil); got.Type != resp.Error {
		t.Errorf("del(no args) = %+v, want an error reply", got)
	}
}

func TestExists(t *testing.T) {
	s := store.New()
	set(s, []string{"a", "1"})

	got := exists(s, []string{"a", "missing"})
	want := resp.IntegerValue(1)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("exists(a, missing) = %+v, want %+v", got, want)
	}

	if got := exists(s, nil); got.Type != resp.Error {
		t.Errorf("exists(no args) = %+v, want an error reply", got)
	}
}

func TestIncrDecr(t *testing.T) {
	s := store.New()

	got := incr(s, []string{"counter"})
	want := resp.IntegerValue(1)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incr(missing key) = %+v, want %+v", got, want)
	}

	got = incr(s, []string{"counter"})
	want = resp.IntegerValue(2)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incr(counter) = %+v, want %+v", got, want)
	}

	got = decr(s, []string{"counter"})
	want = resp.IntegerValue(1)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decr(counter) = %+v, want %+v", got, want)
	}

	if got := incr(s, nil); got.Type != resp.Error {
		t.Errorf("incr(no args) = %+v, want an error reply", got)
	}
	if got := decr(s, nil); got.Type != resp.Error {
		t.Errorf("decr(no args) = %+v, want an error reply", got)
	}
}

func TestIncrNonInteger(t *testing.T) {
	s := store.New()
	set(s, []string{"k", "not a number"})

	if got := incr(s, []string{"k"}); got.Type != resp.Error {
		t.Errorf("incr(non-integer value) = %+v, want an error reply", got)
	}
	if got := decr(s, []string{"k"}); got.Type != resp.Error {
		t.Errorf("decr(non-integer value) = %+v, want an error reply", got)
	}
}

func TestAppend(t *testing.T) {
	s := store.New()

	got := appendCmd(s, []string{"k", "hello"})
	want := resp.IntegerValue(5)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("append(missing key, hello) = %+v, want %+v", got, want)
	}

	got = appendCmd(s, []string{"k", " world"})
	want = resp.IntegerValue(11)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("append(k, ' world') = %+v, want %+v", got, want)
	}

	if got := appendCmd(s, []string{"only-one-arg"}); got.Type != resp.Error {
		t.Errorf("append(wrong arity) = %+v, want an error reply", got)
	}
}

func TestStrlen(t *testing.T) {
	s := store.New()

	got := strlen(s, []string{"missing"})
	want := resp.IntegerValue(0)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("strlen(missing) = %+v, want %+v", got, want)
	}

	set(s, []string{"k", "hello"})
	got = strlen(s, []string{"k"})
	want = resp.IntegerValue(5)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("strlen(k) = %+v, want %+v", got, want)
	}

	if got := strlen(s, nil); got.Type != resp.Error {
		t.Errorf("strlen(no args) = %+v, want an error reply", got)
	}
}

// --- Hashes ---

func TestHSetHGet(t *testing.T) {
	s := store.New()

	got := hset(s, []string{"h", "f1", "v1"})
	want := resp.IntegerValue(1)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("hset(h, f1, v1) = %+v, want %+v", got, want)
	}

	wantGet := resp.BulkStringValue("v1")
	if got := hget(s, []string{"h", "f1"}); !reflect.DeepEqual(got, wantGet) {
		t.Errorf("hget(h, f1) = %+v, want %+v", got, wantGet)
	}

	// Overwriting an existing field returns 0 new fields.
	got = hset(s, []string{"h", "f1", "v2"})
	want = resp.IntegerValue(0)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("hset(overwrite) = %+v, want %+v", got, want)
	}

	if got := hset(s, []string{"h", "only-field"}); got.Type != resp.Error {
		t.Errorf("hset(odd args) = %+v, want an error reply", got)
	}
	if got := hget(s, []string{"h"}); got.Type != resp.Error {
		t.Errorf("hget(wrong arity) = %+v, want an error reply", got)
	}
}

func TestHGetMissing(t *testing.T) {
	want := resp.NullBulkString()
	if got := hget(store.New(), []string{"missing", "f"}); !reflect.DeepEqual(got, want) {
		t.Errorf("hget(missing key) = %+v, want %+v", got, want)
	}

	s := store.New()
	hset(s, []string{"h", "f1", "v1"})
	if got := hget(s, []string{"h", "nope"}); !reflect.DeepEqual(got, want) {
		t.Errorf("hget(missing field) = %+v, want %+v", got, want)
	}
}

func TestHDel(t *testing.T) {
	s := store.New()
	hset(s, []string{"h", "f1", "v1", "f2", "v2"})

	got := hdel(s, []string{"h", "f1", "nope"})
	want := resp.IntegerValue(1)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("hdel(h, f1, nope) = %+v, want %+v", got, want)
	}

	if got := hdel(s, []string{"h"}); got.Type != resp.Error {
		t.Errorf("hdel(wrong arity) = %+v, want an error reply", got)
	}
}

func TestHGetAll(t *testing.T) {
	s := store.New()

	got := hgetall(s, []string{"missing"})
	if got.Type != resp.Array || len(got.Array) != 0 {
		t.Errorf("hgetall(missing) = %+v, want an empty array", got)
	}

	hset(s, []string{"h", "f1", "v1"})
	got = hgetall(s, []string{"h"})
	if got.Type != resp.Array || len(got.Array) != 2 {
		t.Fatalf("hgetall(h) = %+v, want a 2-element array", got)
	}
}

func TestHExists(t *testing.T) {
	s := store.New()
	hset(s, []string{"h", "f1", "v1"})

	want := resp.IntegerValue(1)
	if got := hexists(s, []string{"h", "f1"}); !reflect.DeepEqual(got, want) {
		t.Errorf("hexists(h, f1) = %+v, want %+v", got, want)
	}
	want = resp.IntegerValue(0)
	if got := hexists(s, []string{"h", "nope"}); !reflect.DeepEqual(got, want) {
		t.Errorf("hexists(h, nope) = %+v, want %+v", got, want)
	}
}

func TestHLen(t *testing.T) {
	s := store.New()
	want := resp.IntegerValue(0)
	if got := hlen(s, []string{"missing"}); !reflect.DeepEqual(got, want) {
		t.Errorf("hlen(missing) = %+v, want %+v", got, want)
	}

	hset(s, []string{"h", "f1", "v1", "f2", "v2"})
	want = resp.IntegerValue(2)
	if got := hlen(s, []string{"h"}); !reflect.DeepEqual(got, want) {
		t.Errorf("hlen(h) = %+v, want %+v", got, want)
	}
}

// --- Lists ---

func TestLPushRPush(t *testing.T) {
	s := store.New()

	got := rpush(s, []string{"l", "a", "b", "c"})
	want := resp.IntegerValue(3)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rpush(l, a, b, c) = %+v, want %+v", got, want)
	}

	got = lpush(s, []string{"l2", "a", "b"})
	want = resp.IntegerValue(2)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lpush(l2, a, b) = %+v, want %+v", got, want)
	}

	if got := lpush(s, []string{"l"}); got.Type != resp.Error {
		t.Errorf("lpush(wrong arity) = %+v, want an error reply", got)
	}
	if got := rpush(s, []string{"l"}); got.Type != resp.Error {
		t.Errorf("rpush(wrong arity) = %+v, want an error reply", got)
	}
}

func TestLPopRPop(t *testing.T) {
	s := store.New()
	rpush(s, []string{"l", "a", "b", "c"})

	want := resp.BulkStringValue("a")
	if got := lpop(s, []string{"l"}); !reflect.DeepEqual(got, want) {
		t.Errorf("lpop(l) = %+v, want %+v", got, want)
	}

	want = resp.BulkStringValue("c")
	if got := rpop(s, []string{"l"}); !reflect.DeepEqual(got, want) {
		t.Errorf("rpop(l) = %+v, want %+v", got, want)
	}
}

func TestLPopRPopEmptyOrMissing(t *testing.T) {
	s := store.New()

	want := resp.NullBulkString()
	if got := lpop(s, []string{"missing"}); !reflect.DeepEqual(got, want) {
		t.Errorf("lpop(missing) = %+v, want %+v", got, want)
	}
	if got := rpop(s, []string{"missing"}); !reflect.DeepEqual(got, want) {
		t.Errorf("rpop(missing) = %+v, want %+v", got, want)
	}
}

func TestLRange(t *testing.T) {
	s := store.New()
	rpush(s, []string{"l", "a", "b", "c", "d", "e"})

	got := lrange(s, []string{"l", "0", "-1"})
	want := resp.ArrayValue(
		resp.BulkStringValue("a"), resp.BulkStringValue("b"), resp.BulkStringValue("c"),
		resp.BulkStringValue("d"), resp.BulkStringValue("e"),
	)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lrange(l, 0, -1) = %+v, want %+v", got, want)
	}

	got = lrange(s, []string{"l", "-1", "-1"})
	want = resp.ArrayValue(resp.BulkStringValue("e"))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lrange(l, -1, -1) = %+v, want %+v", got, want)
	}

	if got := lrange(s, []string{"l", "not-an-int", "-1"}); got.Type != resp.Error {
		t.Errorf("lrange(bad index) = %+v, want an error reply", got)
	}
	if got := lrange(s, []string{"l", "0"}); got.Type != resp.Error {
		t.Errorf("lrange(wrong arity) = %+v, want an error reply", got)
	}
}

func TestLLen(t *testing.T) {
	s := store.New()
	want := resp.IntegerValue(0)
	if got := llen(s, []string{"missing"}); !reflect.DeepEqual(got, want) {
		t.Errorf("llen(missing) = %+v, want %+v", got, want)
	}

	rpush(s, []string{"l", "a", "b", "c"})
	want = resp.IntegerValue(3)
	if got := llen(s, []string{"l"}); !reflect.DeepEqual(got, want) {
		t.Errorf("llen(l) = %+v, want %+v", got, want)
	}
}

// --- WRONGTYPE ---

func TestWrongTypeErrors(t *testing.T) {
	s := store.New()
	set(s, []string{"str", "v"})
	hset(s, []string{"hash", "f", "v"})
	rpush(s, []string{"list", "a"})

	wrongTypeCases := []struct {
		name string
		call func() resp.Value
	}{
		{"get on hash", func() resp.Value { return get(s, []string{"hash"}) }},
		{"incr on list", func() resp.Value { return incr(s, []string{"list"}) }},
		{"append on hash", func() resp.Value { return appendCmd(s, []string{"hash", "x"}) }},
		{"strlen on list", func() resp.Value { return strlen(s, []string{"list"}) }},
		{"hset on string", func() resp.Value { return hset(s, []string{"str", "f", "v"}) }},
		{"hget on list", func() resp.Value { return hget(s, []string{"list", "f"}) }},
		{"hdel on string", func() resp.Value { return hdel(s, []string{"str", "f"}) }},
		{"hgetall on string", func() resp.Value { return hgetall(s, []string{"str"}) }},
		{"hexists on list", func() resp.Value { return hexists(s, []string{"list", "f"}) }},
		{"hlen on string", func() resp.Value { return hlen(s, []string{"str"}) }},
		{"lpush on hash", func() resp.Value { return lpush(s, []string{"hash", "x"}) }},
		{"rpush on string", func() resp.Value { return rpush(s, []string{"str", "x"}) }},
		{"lpop on hash", func() resp.Value { return lpop(s, []string{"hash"}) }},
		{"rpop on string", func() resp.Value { return rpop(s, []string{"str"}) }},
		{"lrange on hash", func() resp.Value { return lrange(s, []string{"hash", "0", "-1"}) }},
		{"llen on string", func() resp.Value { return llen(s, []string{"str"}) }},
		{"zadd on string", func() resp.Value { return zadd(s, []string{"str", "1", "m"}) }},
		{"zscore on hash", func() resp.Value { return zscore(s, []string{"hash", "m"}) }},
		{"zrank on list", func() resp.Value { return zrank(s, []string{"list", "m"}) }},
		{"zrange on string", func() resp.Value { return zrange(s, []string{"str", "0", "-1"}) }},
		{"zrem on hash", func() resp.Value { return zrem(s, []string{"hash", "m"}) }},
		{"zincrby on list", func() resp.Value { return zincrby(s, []string{"list", "1", "m"}) }},
	}

	for _, tt := range wrongTypeCases {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.call()
			if got.Type != resp.Error || !strings.HasPrefix(got.Str, "WRONGTYPE") {
				t.Errorf("%s = %+v, want a WRONGTYPE error reply", tt.name, got)
			}
		})
	}
}

// --- Sorted sets ---

func TestZAddZScore(t *testing.T) {
	s := store.New()

	got := zadd(s, []string{"z", "1", "a"})
	want := resp.IntegerValue(1)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("zadd(z, 1, a) = %+v, want %+v", got, want)
	}

	wantScore := resp.BulkStringValue("1")
	if got := zscore(s, []string{"z", "a"}); !reflect.DeepEqual(got, wantScore) {
		t.Errorf("zscore(z, a) = %+v, want %+v", got, wantScore)
	}

	// Updating an existing member's score reports 0 newly added.
	got = zadd(s, []string{"z", "2.5", "a"})
	want = resp.IntegerValue(0)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("zadd(update score) = %+v, want %+v", got, want)
	}
	wantScore = resp.BulkStringValue("2.5")
	if got := zscore(s, []string{"z", "a"}); !reflect.DeepEqual(got, wantScore) {
		t.Errorf("zscore(z, a) after update = %+v, want %+v", got, wantScore)
	}

	if got := zadd(s, []string{"z", "not-a-float", "b"}); got.Type != resp.Error {
		t.Errorf("zadd(bad score) = %+v, want an error reply", got)
	}
	if got := zadd(s, []string{"z", "1"}); got.Type != resp.Error {
		t.Errorf("zadd(wrong arity) = %+v, want an error reply", got)
	}
}

func TestZScoreMissing(t *testing.T) {
	want := resp.NullBulkString()
	if got := zscore(store.New(), []string{"missing", "m"}); !reflect.DeepEqual(got, want) {
		t.Errorf("zscore(missing key) = %+v, want %+v", got, want)
	}
	s := store.New()
	zadd(s, []string{"z", "1", "a"})
	if got := zscore(s, []string{"z", "nope"}); !reflect.DeepEqual(got, want) {
		t.Errorf("zscore(missing member) = %+v, want %+v", got, want)
	}
}

func TestZRank(t *testing.T) {
	s := store.New()
	zadd(s, []string{"z", "30", "c", "10", "a", "20", "b"})

	tests := []struct {
		member   string
		wantRank int64
	}{
		{"a", 0},
		{"b", 1},
		{"c", 2},
	}
	for _, tt := range tests {
		want := resp.IntegerValue(tt.wantRank)
		if got := zrank(s, []string{"z", tt.member}); !reflect.DeepEqual(got, want) {
			t.Errorf("zrank(z, %s) = %+v, want %+v", tt.member, got, want)
		}
	}

	want := resp.NullBulkString()
	if got := zrank(s, []string{"z", "nope"}); !reflect.DeepEqual(got, want) {
		t.Errorf("zrank(missing member) = %+v, want %+v", got, want)
	}
}

func TestZRange(t *testing.T) {
	s := store.New()
	zadd(s, []string{"z", "1", "a", "2", "b", "3", "c"})

	got := zrange(s, []string{"z", "0", "-1"})
	want := resp.ArrayValue(resp.BulkStringValue("a"), resp.BulkStringValue("b"), resp.BulkStringValue("c"))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("zrange(z, 0, -1) = %+v, want %+v", got, want)
	}

	got = zrange(s, []string{"z", "-1", "-1"})
	want = resp.ArrayValue(resp.BulkStringValue("c"))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("zrange(z, -1, -1) = %+v, want %+v", got, want)
	}

	if got := zrange(s, []string{"z", "bad", "-1"}); got.Type != resp.Error {
		t.Errorf("zrange(bad index) = %+v, want an error reply", got)
	}
}

func TestZRem(t *testing.T) {
	s := store.New()
	zadd(s, []string{"z", "1", "a", "2", "b"})

	got := zrem(s, []string{"z", "a", "nope"})
	want := resp.IntegerValue(1)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("zrem(z, a, nope) = %+v, want %+v", got, want)
	}

	if got := zrem(s, []string{"z"}); got.Type != resp.Error {
		t.Errorf("zrem(wrong arity) = %+v, want an error reply", got)
	}
}

func TestZIncrBy(t *testing.T) {
	s := store.New()

	got := zincrby(s, []string{"z", "5", "a"})
	want := resp.BulkStringValue("5")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("zincrby(missing member, 5) = %+v, want %+v", got, want)
	}

	got = zincrby(s, []string{"z", "2.5", "a"})
	want = resp.BulkStringValue("7.5")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("zincrby(a, 2.5) = %+v, want %+v", got, want)
	}

	if got := zincrby(s, []string{"z", "not-a-float", "a"}); got.Type != resp.Error {
		t.Errorf("zincrby(bad increment) = %+v, want an error reply", got)
	}
}
