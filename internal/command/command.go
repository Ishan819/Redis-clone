// Package command implements Redis commands. Each command is a Handler
// that takes its arguments (the command name itself excluded) and returns
// the RESP reply to send back to the client.
package command

import (
	"strings"

	"github.com/Ishan819/Redis-clone/internal/resp"
)

// Handler executes a single command and returns the RESP reply for it.
type Handler func(args []string) resp.Value

// registry maps upper-cased command names to their handlers.
var registry = map[string]Handler{
	"PING": ping,
	"ECHO": echo,
}

// Lookup returns the handler registered for name, matched case-insensitively
// (as Redis commands are), or nil if name isn't a known command.
func Lookup(name string) Handler {
	return registry[strings.ToUpper(name)]
}

// ping implements PING: with no arguments it replies +PONG; with one
// argument it echoes that argument back, matching real Redis behavior.
func ping(args []string) resp.Value {
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
func echo(args []string) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'echo' command")
	}
	return resp.BulkStringValue(args[0])
}
