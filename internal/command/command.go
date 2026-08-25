// Package command implements Redis commands. Each command is a Handler
// that takes the shared store plus its arguments (the command name itself
// excluded) and returns the RESP reply to send back to the client.
package command

import (
	"strings"

	"github.com/Ishan819/Redis-clone/internal/resp"
	"github.com/Ishan819/Redis-clone/internal/store"
)

// Handler executes a single command against s and returns the RESP reply
// for it.
type Handler func(s *store.Store, args []string) resp.Value

// registry maps upper-cased command names to their handlers.
var registry = map[string]Handler{
	"PING": ping,
	"ECHO": echo,

	"SET":    set,
	"GET":    get,
	"DEL":    del,
	"EXISTS": exists,
	"INCR":   incr,
	"DECR":   decr,
	"APPEND": appendCmd,
	"STRLEN": strlen,
}

// Lookup returns the handler registered for name, matched case-insensitively
// (as Redis commands are), or nil if name isn't a known command.
func Lookup(name string) Handler {
	return registry[strings.ToUpper(name)]
}

// ping implements PING: with no arguments it replies +PONG; with one
// argument it echoes that argument back, matching real Redis behavior.
func ping(_ *store.Store, args []string) resp.Value {
	switch len(args) {
	case 0:
		return resp.SimpleStringValue("PONG")
	case 1:
		return resp.BulkStringValue(args[0])
	default:
		return resp.ErrorValue("ERR wrong number of arguments for 'ping' command")
	}
}

// echo implements ECHO: it requires exactly one argument, which it returns
// unchanged as a bulk string.
func echo(_ *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'echo' command")
	}
	return resp.BulkStringValue(args[0])
}

// set implements SET key value: it unconditionally stores value under key
// and replies +OK. (Optional SET flags like EX/NX are not implemented yet.)
func set(s *store.Store, args []string) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'set' command")
	}
	s.Set(args[0], args[1])
	return resp.SimpleStringValue("OK")
}

// get implements GET key: it returns the stored value as a bulk string, or
// a null bulk string if key doesn't exist.
func get(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'get' command")
	}
	v, ok := s.Get(args[0])
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkStringValue(v)
}

// del implements DEL key [key ...]: it removes each given key and returns
// the number of keys actually removed.
func del(s *store.Store, args []string) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'del' command")
	}
	return resp.IntegerValue(int64(s.Del(args...)))
}

// exists implements EXISTS key [key ...]: it returns how many of the given
// keys are present (counting repeats).
func exists(s *store.Store, args []string) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'exists' command")
	}
	return resp.IntegerValue(int64(s.Exists(args...)))
}

// incr implements INCR key: it increments the integer value at key by 1
// (treating a missing key as 0) and returns the new value. It errors if the
// existing value isn't an integer.
func incr(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'incr' command")
	}
	n, err := s.Incr(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

// decr implements DECR key: the same as incr but subtracting 1.
func decr(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'decr' command")
	}
	n, err := s.Decr(args[0])
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

// appendCmd implements APPEND key value: it appends value to the existing
// string at key (treating a missing key as empty) and returns the length
// of the result. Named appendCmd since append is a Go builtin.
func appendCmd(s *store.Store, args []string) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'append' command")
	}
	return resp.IntegerValue(int64(s.Append(args[0], args[1])))
}

// strlen implements STRLEN key: it returns the length of the string stored
// at key, or 0 if key doesn't exist.
func strlen(s *store.Store, args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'strlen' command")
	}
	return resp.IntegerValue(int64(s.Strlen(args[0])))
}
