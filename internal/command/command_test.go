package command

import (
	"reflect"
	"testing"

	"github.com/Ishan819/Redis-clone/internal/resp"
)

func TestLookupIsCaseInsensitive(t *testing.T) {
	for _, name := range []string{"ping", "PING", "PiNg"} {
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
			got := ping(tt.args)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ping(%v) = %+v, want %+v", tt.args, got, tt.want)
			}
		})
	}

	if got := ping([]string{"too", "many"}); got.Type != resp.Error {
		t.Errorf("ping(too many args) = %+v, want an error reply", got)
	}
}

func TestEcho(t *testing.T) {
	want := resp.BulkStringValue("hello")
	if got := echo([]string{"hello"}); !reflect.DeepEqual(got, want) {
		t.Errorf("echo([hello]) = %+v, want %+v", got, want)
	}

	if got := echo(nil); got.Type != resp.Error {
		t.Errorf("echo(no args) = %+v, want an error reply", got)
	}
	if got := echo([]string{"a", "b"}); got.Type != resp.Error {
		t.Errorf("echo(too many args) = %+v, want an error reply", got)
	}
}
