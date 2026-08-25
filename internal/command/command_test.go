package command

import (
	"reflect"
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
